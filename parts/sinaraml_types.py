from enum import Enum
import json
import os

class SinaraPipelineType(Enum):
    ML = 'ml'
    CV = 'cv'

    def __str__(self):
        return self.value

org = json.loads(os.environ["SINARA_ORG"].replace("'", '"'))
org_body = org["cli_bodies"][0]

dataflow_fabric_default_repo = {
  'url': 'https://github.com/4-DS/dataflow_fabric_ml_default.git',
  'username': '',
  'password': ''
}
step_template_default_repo = {
  'url': 'https://github.com/4-DS/pipeline-step_template.git',
  'provider_organization_api': org_body["git_api"], #'https://api.github.com',
  'provider_organization_url': org_body["git_url"], #'https://github.com',
  'provider_type': org_body["git_provider"],
  'username': '',
  'password': ''
}

step_template_default_substep_notebook = 'do_step.ipynb'
    
# dataflow_fabric_default_repos = {
#   SinaraPipelineType.ML: {
#       'url': 'https://github.com/4-DS/dataflow_fabric_ml_default.git',
#       'username': '',
#       'password': ''
#       },
#   SinaraPipelineType.CV: {
#       'url': 'https://github.com/4-DS/dataflow_fabric_cv_rest.git',
#       'username': '',
#       'password': ''
#       }
# }

# step_template_default_repo = {
#   SinaraPipelineType.ML: {
#       'url': 'https://github.com/4-DS/pipeline-step_template.git',
#       'provider_organization_api': 'https://api.github.com',
#       'provider_organization_url': 'https://github.com',
#       'username': '',
#       'password': ''
#       },
#   SinaraPipelineType.CV: {
#       'url': 'https://github.com/4-DS/pipeline-step_template.git',
#       'provider_organization_api': 'https://api.github.com',
#       'provider_organization_url': 'https://github.com',
#       'username': '',
#       'password': ''
#       }
# }

# step_template_default_substep_notebook = {
#     SinaraPipelineType.ML: 'do_step.ipynb',
#     SinaraPipelineType.CV: 'do_step.ipynb' 
# }