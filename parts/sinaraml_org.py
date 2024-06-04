import json
import os

class SinaraOrg():

    org_body = {}
    def __init__():
        org = json.loads(os.environ["SINARA_ORG"])
        org_body = org["cli_bodies"][0]
        
    def get_git_api():
        return org_body["git_api"]

    def get_git_url():
        return org_body["git_url"]

    def get_git_provider():
         return org_body["git_provider"]