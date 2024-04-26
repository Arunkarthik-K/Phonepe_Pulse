from git import Repo, GitCommandError
import dataExtraction

github_url = "https://github.com/PhonePe/pulse.git"
local_dir = "/Users/arunkarthik/Projects/Personal/phonepe/Dataset"


def cloneData():
    try:
        Repo.clone_from(github_url, local_dir)
        print("Repository cloned successfully!")
        dataExtraction.extractData()
    except GitCommandError as e:
        if e.status == 128:
            print("Directory already exists. Pulling latest changes...")
            repo = Repo(local_dir)
            origin = repo.remote('origin')
            origin.pull()
            print("Latest changes pulled successfully!")
            dataExtraction.extractData()
        else:
            print("An error occurred:", e)
