import re
import shutil
import os

import git
from tenacity import retry, stop_after_attempt, wait_exponential

from utils import cov2sec, is_40_hex_chars, sanitize_branch_name


class Mirror(object):
    def __init__(
        self, hub, src_name, dst_name,
        cache='.', timeout='0', force_update=False, lfs=False,
        branch=None, shallow_clone=False
    ):
        self.hub = hub
        self.src_name = src_name
        self.dst_name = dst_name
        self.src_url = hub.src_repo_base + '/' + src_name + ".git"
        self.dst_url = hub.dst_repo_base + '/' + dst_name + ".git"
        self.repo_path = cache + '/' + src_name
        self.branch = branch
        self.shallow_clone = shallow_clone
        if re.match(r"^\d+[dhms]?$", timeout):
            self.timeout = cov2sec(timeout)
        else:
            self.timeout = 0
        self.force_update = force_update
        self.lfs = lfs

    @retry(wait=wait_exponential(), reraise=True, stop=stop_after_attempt(3))
    def _clone(self):
        # TODO: process empty repo
        print("Starting git clone " + self.src_url)
        mygit = git.cmd.Git(os.getcwd())

        clone_args = [git.cmd.Git.polish_url(self.src_url), self.repo_path]

        # Add branch and shallow clone options if specified
        if self.branch:
            clone_args.extend(["--branch", self.branch])
        if self.shallow_clone:
            clone_args.extend(["--depth", "1"])

        mygit.clone(*clone_args, kill_after_timeout=self.timeout)
        local_repo = git.Repo(self.repo_path)

        if self.lfs:
            local_repo.git.lfs("fetch", "--all", "origin")

        if self.shallow_clone:
            # Amend the last commit
            head_commit = local_repo.head.commit
            
            # Track large files with git lfs
            for root, _, files in os.walk(self.repo_path):
                if '.git' in root:
                    continue
                for filename in files:
                    file_path = os.path.join(root, filename)
                    if os.path.islink(file_path): # Skip symbolic links
                        continue
                    if os.path.getsize(file_path) > 100 * 1024 * 1024:  # 100MB
                        # Remove large file from head_commit
                        local_repo.index.remove([file_path])
                        local_repo.index.commit(f"Remove large file {filename} from history and track with git lfs")
                        local_repo.git.lfs("track", filename)
                        print(f"Tracking large file with git lfs: {filename}")
                        local_repo.git.add(file_path)
                        local_repo.git.add(".gitattributes")
                        local_repo.index.commit(f"Add large file {filename} back with git lfs tracking")

            # Create a new commit with the same tree and message as the old one
            local_repo.index.write()  # Ensure the index is written
            new_commit = local_repo.index.commit(
                head_commit.message,  # Use the same commit message
                author=head_commit.author,
                committer=head_commit.committer,
                parent_commits=[],  # This makes it an initial commit
                skip_hooks=True  # Skip pre-commit and post-commit hooks
            )

            # Force the HEAD reference to point to the new commit
            local_repo.head.reference.set_commit(new_commit)
            local_repo.head.reset(index=True, working_tree=True)
            print(f"Amended commit: {new_commit.hexsha}")

        print("Clone completed: %s" % (os.getcwd() + self.repo_path))

    @retry(wait=wait_exponential(), reraise=True, stop=stop_after_attempt(3))
    def _update(self, local_repo):
        try:
            local_repo.git.pull(kill_after_timeout=self.timeout)
            if self.lfs:
                local_repo.git.lfs("fetch", "--all", "origin")
        except git.exc.GitCommandError:
            # Cleanup local repo and re-clone
            print('Updating failed, re-clone %s' % self.src_name)
            shutil.rmtree(local_repo.working_dir)
            self._clone()

    @retry(wait=wait_exponential(), reraise=True, stop=stop_after_attempt(3))
    def download(self):
        print("(1/3) Downloading...")
        try:
            local_repo = git.Repo(self.repo_path)
        except git.exc.NoSuchPathError:
            self._clone()
        else:
            print("Updating repo...")
            self._update(local_repo)

    def create(self):
        print("(2/3) Creating...")
        self.hub.create_dst_repo(self.dst_name)

    def _check_empty(self, repo):
        cmd = ["-n", "1", "--all"]
        if repo.git.rev_list(*cmd):
            return False
        else:
            return True

    def _sanitize_problematic_branches(self, local_repo):
        """
        Create sanitized local branches for any remote branches that have 40-hex-char names.
        GitHub doesn't allow branch names that consist of 40 hexadecimal characters.
        """
        problematic_branches = []
        
        # Get all remote branches
        for remote_ref in local_repo.remotes.origin.refs:
            branch_name = remote_ref.name.split('/')[-1]  # Extract branch name from refs/remotes/origin/branch_name
            
            if is_40_hex_chars(branch_name):
                sanitized_name = sanitize_branch_name(branch_name)
                problematic_branches.append((branch_name, sanitized_name))
                
                print(f"Found problematic branch '{branch_name}', creating sanitized branch '{sanitized_name}'")
                
                # Create a local branch with the sanitized name pointing to the same commit
                try:
                    # Delete existing local branch if it exists
                    if sanitized_name in [head.name for head in local_repo.heads]:
                        local_repo.delete_head(sanitized_name, force=True)
                    
                    # Create new local branch
                    new_branch = local_repo.create_head(sanitized_name, remote_ref.commit)
                    print(f"Created local branch '{sanitized_name}' -> {remote_ref.commit.hexsha}")
                except Exception as e:
                    print(f"Warning: Failed to create sanitized branch '{sanitized_name}': {e}")
        
        return problematic_branches

    @retry(wait=wait_exponential(), reraise=True, stop=stop_after_attempt(3))
    def push(self, force=False):
        local_repo = git.Repo(self.repo_path)
        git_cmd = local_repo.git
        if self._check_empty(local_repo):
            print("Empty repo %s, skip pushing." % self.src_url)
            return
        cmd = ['set-head', 'origin', '-d']
        local_repo.git.remote(*cmd)
        try:
            local_repo.create_remote(self.hub.dst_type, self.dst_url)
        except git.exc.GitCommandError:
            print("Remote exists, re-create: set %s to %s" % (
                self.hub.dst_type, self.dst_url))
            local_repo.delete_remote(self.hub.dst_type)
            local_repo.create_remote(self.hub.dst_type, self.dst_url)
        
        if self.branch:
            # When pushing a specific branch, don't sanitize - push as-is
            cmd = [
                self.hub.dst_type, f'{self.branch}'
            ]
        else:
            # Handle problematic branch names before pushing all branches
            problematic_branches = self._sanitize_problematic_branches(local_repo)
            
            # Push all branches, but we need to handle problematic ones individually
            cmd_args = []
            
            # First, push all non-problematic branches using the normal refspec
            if not problematic_branches:
                cmd_args = [
                    self.hub.dst_type, 'refs/remotes/origin/*:refs/heads/*',
                    '--tags', '--prune'
                ]
            else:
                # Push normal branches first
                normal_branches = []
                for remote_ref in local_repo.remotes.origin.refs:
                    branch_name = remote_ref.name.split('/')[-1]
                    if not is_40_hex_chars(branch_name):
                        normal_branches.append(f'refs/remotes/origin/{branch_name}:refs/heads/{branch_name}')
                
                if normal_branches:
                    cmd_args = [self.hub.dst_type] + normal_branches + ['--tags', '--prune']
                
            cmd = cmd_args
        
        # Push normal branches or specific branch
        if cmd and len(cmd) > 1:  # Make sure we have something to push
            if not self.force_update:
                print("(3/3) Pushing...")
                local_repo.git.push(*cmd, kill_after_timeout=self.timeout)
            else:
                print("(3/3) Force pushing...")
                cmd = ['-f'] + cmd
                local_repo.git.push(*cmd, kill_after_timeout=self.timeout)
        
        # Only push sanitized branches when mirroring all branches (not when self.branch is specified)
        if not self.branch:
            problematic_branches = locals().get('problematic_branches', [])
            for original_name, sanitized_name in problematic_branches:
                print(f"Pushing sanitized branch '{sanitized_name}' (was '{original_name}')")
                sanitized_cmd = [self.hub.dst_type, f'{sanitized_name}:{sanitized_name}']
                if self.force_update:
                    sanitized_cmd = ['-f'] + sanitized_cmd
                try:
                    local_repo.git.push(*sanitized_cmd, kill_after_timeout=self.timeout)
                    print(f"Successfully pushed sanitized branch '{sanitized_name}'")
                except git.exc.GitCommandError as e:
                    print(f"Failed to push sanitized branch '{sanitized_name}': {e}")
        
        # Push LFS files if needed
        if self.lfs:
            git_cmd.lfs("push", self.hub.dst_type, "--all")
