import re
import shutil
import os

import git
from tenacity import retry, stop_after_attempt, wait_exponential

from utils import cov2sec


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
            cmd = [
            self.hub.dst_type, f'{self.branch}'
            ]
        else:
            cmd = [
            self.hub.dst_type, 'refs/remotes/origin/*:refs/heads/*',
            '--tags', '--prune'
            ]
        if not self.force_update:
            print("(3/3) Pushing...")
            local_repo.git.push(*cmd, kill_after_timeout=self.timeout)
            if self.lfs:
                git_cmd.lfs("push", self.hub.dst_type, "--all")
        else:
            print("(3/3) Force pushing...")
            if self.lfs:
                git_cmd.lfs("push", self.hub.dst_type, "--all")
            cmd = ['-f'] + cmd
            local_repo.git.push(*cmd, kill_after_timeout=self.timeout)
