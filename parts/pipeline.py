from .sinaraml_types import SinaraPipelineType, \
                            dataflow_fabric_default_repo, \
                            step_template_default_repo, \
                            step_template_default_substep_notebook
import subprocess
import tempfile
import os
import shutil
import logging
from urllib.parse import urlparse, unquote
from pathlib import Path
from getpass import getpass

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class SinaraPipeline():

    subject = 'pipeline'
    root_parser = None
    subject_parser = None
    create_parser = None
    pull_parser = None
    push_parser = None
    update_parser = None

    @staticmethod
    def add_command_handlers(root_parser, subject_parser):
        SinaraPipeline.root_parser = root_parser
        SinaraPipeline.subject_parser = subject_parser
        parser_pipeline = subject_parser.add_parser(SinaraPipeline.subject, help='sinara pipeline subject')
        pipeline_subparsers = parser_pipeline.add_subparsers(title='action', dest='action', help='Action to do with subject')

        SinaraPipeline.add_create_handler(pipeline_subparsers)
        SinaraPipeline.add_pull_handler(pipeline_subparsers)
        SinaraPipeline.add_push_handler(pipeline_subparsers)
        SinaraPipeline.add_update_handler(pipeline_subparsers)
        SinaraPipeline.add_checkout_handler(pipeline_subparsers)

    @staticmethod
    def add_create_handler(pipeline_cmd_parser):
        SinaraPipeline.create_parser = pipeline_cmd_parser.add_parser('create', help='Create SinaraML Pipeline')
        SinaraPipeline.create_parser.add_argument('--type', type=SinaraPipelineType, choices=list(SinaraPipelineType), help='sinara pipeline type (default: %(default)s)')
        SinaraPipeline.create_parser.add_argument('--name', type=str, help='pipeline name', required=True)
        SinaraPipeline.create_parser.add_argument('--git_username', type=str, help='pipeline git repo user name')
        SinaraPipeline.create_parser.add_argument('--git_password', type=str, help='pipeline git repo user password')
        SinaraPipeline.create_parser.add_argument('--git_email', type=str, help='pipeline git repo user email')
        #SinaraPipeline.create_parser.add_argument('--step_template', type=str, help='sinara step template repo url')
        #SinaraPipeline.create_parser.add_argument('--step_template_git_user', type=str, help='sinara fabric repo git user name')
        #SinaraPipeline.create_parser.add_argument('--step_template_git_password', type=str, help='sinara fabric repo git password')
        #SinaraPipeline.create_parser.add_argument('--step_template_provider_organization_api', type=str, help='sinara step template repo git provider api url')
        #SinaraPipeline.create_parser.add_argument('--step_template_provider_organization_url', type=str, help='sinara step template repo git provider organization url')
        SinaraPipeline.create_parser.set_defaults(func=SinaraPipeline.create)

    @staticmethod
    def add_pull_handler(pipeline_cmd_parser):
        SinaraPipeline.pull_parser = pipeline_cmd_parser.add_parser('pull', help='pull SinaraML Pipeline')
        SinaraPipeline.pull_parser.add_argument('--git_username', type=str, help='pipeline git repo user name')
        SinaraPipeline.pull_parser.add_argument('--git_password', type=str, help='pipeline git repo password')
        SinaraPipeline.pull_parser.add_argument('--pipeline_git_url', type=str, help='url of the pipeline "folder" in the git repo')
        #SinaraPipeline.pull_parser.add_argument('--step_template_provider_organization_api', type=str, help='sinara step template repo git provider api url')
        #SinaraPipeline.pull_parser.add_argument('--step_template_provider_organization_url', type=str, help='sinara step template repo git provider organization url')
        SinaraPipeline.pull_parser.set_defaults(func=SinaraPipeline.pull)

    @staticmethod
    def add_push_handler(pipeline_cmd_parser):
        SinaraPipeline.push_parser = pipeline_cmd_parser.add_parser('push', help='push sinara pipeline')
        SinaraPipeline.push_parser.add_argument('--git_username', type=str, help='pipeline git repo user name')
        SinaraPipeline.push_parser.add_argument('--git_password', type=str, help='pipeline git repo user password or token')
        SinaraPipeline.push_parser.add_argument('--pipeline_git_url', type=str, help='url of the pipeline "folder" in the git repo')
        #SinaraPipeline.push_parser.add_argument('--git_provider_api', type=str, help='sinara step template repo git provider api url')
        #SinaraPipeline.push_parser.add_argument('--git_provider_url', type=str, help='sinara step template repo git provider organization url')
        SinaraPipeline.push_parser.set_defaults(func=SinaraPipeline.push)

    @staticmethod
    def add_update_handler(pipeline_cmd_parser):
        SinaraPipeline.update_parser = pipeline_cmd_parser.add_parser('update', help='update sinara pipeline components')
        SinaraPipeline.update_parser.add_argument('--component', choices=['sinaralib', 'origin'], type=str, help='sinara component to update')
        SinaraPipeline.update_parser.add_argument('--git_username', type=str, help='sinara fabric repo git user name')
        SinaraPipeline.update_parser.add_argument('--git_password', type=str, help='sinara fabric repo git password')
        SinaraPipeline.update_parser.add_argument('--new_origin_url', type=str, help='"new git origin url for pipeline steps')
        SinaraPipeline.update_parser.set_defaults(func=SinaraPipeline.update)

    @staticmethod
    def add_checkout_handler(pipeline_cmd_parser):
        SinaraPipeline.checkout_parser = pipeline_cmd_parser.add_parser('checkout', help='checkout a specific branch in all sinara pipeline components')
        SinaraPipeline.checkout_parser.add_argument('--git_username', type=str, help='sinara fabric repo git user name')
        SinaraPipeline.checkout_parser.add_argument('--git_password', type=str, help='sinara fabric repo git password')
        SinaraPipeline.checkout_parser.add_argument('--step_template_provider_organization_api', type=str, help='sinara step template repo git provider api url')
        SinaraPipeline.checkout_parser.add_argument('--step_template_provider_organization_url', type=str, help='sinara step template repo git provider organization url')
        SinaraPipeline.checkout_parser.add_argument('--git_branch', type=str, help='sinara step template branch')
        SinaraPipeline.checkout_parser.add_argument('--steps_folder_glob', type=str, help='sinara steps folder glob pattern')
        SinaraPipeline.checkout_parser.set_defaults(func=SinaraPipeline.checkout)

    @staticmethod
    def ensure_dataflow_fabric_repo_exists(args):
        fabric_repo_url, fabric_repo_username, fabric_repo_password = SinaraPipeline.get_fabric_repo(args)
        repo_folder = Path(__file__).parent.resolve() / 'fabric'

        if os.environ.get('SINARA_DEBUG') == '1' and repo_folder.exists():
            return repo_folder
        
        if repo_folder.exists():
            shutil.rmtree(repo_folder)
        repo_folder.mkdir(parents=True, exist_ok=True)

        git_cmd = f"git -c credential.helper=\'!f() {{ sleep 1; echo \"username=${{GIT_USER}}\"; echo \"password=${{GIT_PASSWORD}}\"; }}; f\' clone --recursive {fabric_repo_url} {repo_folder}"

        temp_env = os.environ.copy()
        temp_env["GIT_USER"] = fabric_repo_username
        temp_env["GIT_PASSWORD"] = fabric_repo_password
        process = subprocess.run(git_cmd,
                                 cwd=repo_folder,
                                 universal_newlines=True,
                                 shell=True,
                                 env=dict(temp_env))
        if process.returncode != 0:
            raise Exception(git_cmd)

        return repo_folder

    @staticmethod
    def call_dataflow_fabric_command(dataflow_fabric_command, work_dir):
        process = subprocess.run(dataflow_fabric_command, cwd=work_dir, universal_newlines=True, shell=True, env=dict(os.environ))
        if process.returncode != 0:
            raise Exception(dataflow_fabric_command)
        
    @staticmethod
    def ensure_pipeline_type(args, command):
        type_input = None
        while not type_input:
            try:
                type_input = int(input(f"Please, enter pipeline type to {command} [1] ML [2] CV: "))
            except:
                type_input = None
        if type_input == 1:
            args.type = SinaraPipelineType.ML
        elif type_input == 2:
            args.type = SinaraPipelineType.CV
        else:
            args.type = None


    @staticmethod
    def read_stored_git_creds(git_repo_url):
        username = None
        password = None
        git_credentials = Path.home() / '.git-credentials'
        if git_credentials.exists():
            print(git_credentials)
            with open(git_credentials, 'r') as f:
                lines = f.readlines()
            for l in lines:
                url = urlparse(l)
                if urlparse(git_repo_url).netloc in url.netloc:
                    username = unquote(url.username)
                    password = unquote(url.password)
        # print(f"user: {username}, password: {password}")
        # exit(0)
        if username and password:
            logger.info('Using stored GIT credentials')
        return username, password
    
    @staticmethod
    def get_step_template_repo(args):
        repo_url = step_template_default_repo['url'] \
            if not 'step_template' in args or not args.step_template else args.step_template
        
        repo_user = step_template_default_repo['username'] \
             if not 'git_username' in args or not args.git_username else args.git_username
        
        repo_password = step_template_default_repo['password'] \
            if not 'git_password' in args or not args.git_password else args.git_password
        
        repo_provider_organization_api = step_template_default_repo['provider_organization_api'] \
            if not 'git_provider_api' in args or not args.git_provider_api else args.git_provider_api

        repo_provider_organization_url = step_template_default_repo['provider_organization_url'] \
            if not 'git_provider_url' in args or not args.git_provider_url else args.git_provider_url
        
        step_template_provider_type = step_template_default_repo['provider_type'] \
            if not 'git_provider_type' in args or not args.git_provider_type else args.git_provider_type

        if repo_user and not repo_password:
            repo_password = getpass("Git password: ")

        if not repo_user and not repo_password:
            repo_user, repo_password = SinaraPipeline.read_stored_git_creds(repo_provider_organization_url)

        return repo_url, repo_user, repo_password, \
               repo_provider_organization_api, repo_provider_organization_url, step_template_provider_type

    @staticmethod
    def get_fabric_repo(args):
        repo_url = dataflow_fabric_default_repo['url'] \
            if not 'fabric' in args or not args.fabric else args.fabric
        
        repo_user = dataflow_fabric_default_repo['username'] \
            if not args.git_username else args.git_username
        
        repo_password = dataflow_fabric_default_repo['password'] \
            if not args.git_password else args.git_password

        return repo_url, repo_user, repo_password

    @staticmethod
    def create(args):
        curr_dir = os.getcwd()

        if not args.type:
            args.type = 'cv' if 'sinara-cv' in os.environ.get('JUPYTER_IMAGE_SPEC') else 'ml'
            while not args.type:
                SinaraPipeline.ensure_pipeline_type(args, "create")
        args.type = str(args.type).lower()

        step_template_url, step_template_username, \
             step_template_password, \
             step_template_provider_organization_api, \
             step_template_provider_organization_url, \
             step_template_provider_type = SinaraPipeline.get_step_template_repo(args)
        substep_name = step_template_default_substep_notebook
        
        create_pipeline_cmd = f"python sinara_pipeline_create.py "\
                              f"--pipeline_type={args.type} "\
                              f"--pipeline_name={args.name} "\
                              f"--git_step_template_url={step_template_url} "\
                              f"--git_step_template_username={step_template_username} "\
                              f"--git_step_template_password={step_template_password} "\
                              f"--step_template_nb_substep={substep_name} "\
                              f"--pipeline_dir={curr_dir} "\
                              f"--git_username={step_template_username} "\
                              f"--git_provider={step_template_provider_type}"
        logger.debug(create_pipeline_cmd)

        try:
            repo_folder = SinaraPipeline.ensure_dataflow_fabric_repo_exists(args)
            SinaraPipeline.call_dataflow_fabric_command(create_pipeline_cmd, repo_folder)
        except Exception as e:
            logging.debug(e)
            raise Exception('Error while executing fabric scripts, launch CLI with --verbose to see details')

    @staticmethod
    def pull(args):

        step_template_url, step_template_username, \
             step_template_password, \
             step_template_provider_organization_api, \
             step_template_provider_organization_url, \
             step_template_provider_type = SinaraPipeline.get_step_template_repo(args)

        pipeline_git_url = '' if not "pipeline_git_url" in args else args.pipeline_git_url

        curr_dir = os.getcwd()
        pull_pipeline_cmd = f"python sinara_pipeline_pull.py "\
                            f"--pipeline_dir={curr_dir} "\
                            f"--pipeline_git_url={pipeline_git_url} "\
                            f"--git_provider={step_template_provider_type} " \
                            f"--git_provider_api={step_template_provider_organization_api} " \
                            f"--git_provider_url={step_template_provider_organization_url} " \
                            f"--git_username={step_template_username} "\
                            f"--git_password={step_template_password} "
        try:
            repo_folder = SinaraPipeline.ensure_dataflow_fabric_repo_exists(args)
            SinaraPipeline.call_dataflow_fabric_command(pull_pipeline_cmd, repo_folder)
        except Exception as e:
            logging.debug(e)
            raise Exception('Error while executing fabric scripts, launch CLI with --verbose to see details')

    @staticmethod
    def push(args):

        step_template_url, step_template_username, \
             step_template_password, \
             step_template_provider_organization_api, \
             step_template_provider_organization_url, \
             step_template_provider_type = SinaraPipeline.get_step_template_repo(args)
        
        pipeline_git_url = '' if not "pipeline_git_url" in args else args.pipeline_git_url

        curr_dir = os.getcwd()
        push_pipeline_cmd = f"python sinara_pipeline_push.py "\
                            f"--pipeline_dir={curr_dir} "\
                            f"--pipeline_git_url={pipeline_git_url} "\
                            f"--git_username={step_template_username} " \
                            f"--git_password={step_template_password} " \
                            f"--git_provider={step_template_provider_type} " \
                            f"--git_provider_api={step_template_provider_organization_api} " \
                            f"--git_provider_url={step_template_provider_organization_url}"
        
        logger.debug(push_pipeline_cmd)

        try:
            repo_folder = SinaraPipeline.ensure_dataflow_fabric_repo_exists(args)
            SinaraPipeline.call_dataflow_fabric_command(push_pipeline_cmd, repo_folder)
        except Exception as e:
            logging.debug(e)
            raise Exception('Error while executing fabric scripts, launch CLI with --verbose to see details')

    @staticmethod
    def update(args):
        if args.component == "sinaralib":
            SinaraPipeline._update_sinaralib(args)
        elif args.component == "origin":
            SinaraPipeline._update_origin(args)
        else:
            raise Exception(f'Component {args.component} is not supported by update')

    @staticmethod
    def _update_sinaralib(args):
        step_template_url, step_template_username, \
             step_template_password, \
             step_template_provider_api, \
             step_template_provider_url, \
             step_template_provider_type = SinaraPipeline.get_step_template_repo(args)

        curr_dir = os.getcwd()
        update_sinaralib_pipeline_cmd = f"python sinara_pipeline_update_sinaralib.py "\
                                        f"--pipeline_dir={curr_dir} "\
                                        f"--git_provider_type={step_template_provider_type} "\
                                        f"--git_provider_api={step_template_provider_api} "\
                                        f"--git_provider_url={step_template_provider_url} "\
                                        f"--git_username={step_template_username} "\
                                        f"--git_password={step_template_password}"
        try:
            repo_folder = SinaraPipeline.ensure_dataflow_fabric_repo_exists(args)
            SinaraPipeline.call_dataflow_fabric_command(update_sinaralib_pipeline_cmd, repo_folder)
        except Exception as e:
            logging.debug(e)
            raise Exception('Error while executing fabric scripts, launch CLI with --verbose to see details')

    @staticmethod
    def _update_origin(args):
        #git remote set-url origin new.git.url/here
        step_template_url, step_template_username, \
             step_template_password, \
             step_template_provider_api, \
             step_template_provider_url, \
             step_template_provider_type = SinaraPipeline.get_step_template_repo(args)
        
        new_origin_url = args.new_origin_url

        curr_dir = os.getcwd()
        update_origin_pipeline_cmd = f"python sinara_pipeline_update_origin.py "\
                                        f"--pipeline_dir={curr_dir} "\
                                        f"--git_provider_type={step_template_provider_type} "\
                                        f"--git_provider_api={step_template_provider_api} "\
                                        f"--git_provider_url={step_template_provider_url} "\
                                        f"--git_username={step_template_username} "\
                                        f"--git_password={step_template_password} "\
                                        f"--new_origin_url={new_origin_url}"
        try:
            repo_folder = SinaraPipeline.ensure_dataflow_fabric_repo_exists(args)
            SinaraPipeline.call_dataflow_fabric_command(update_origin_pipeline_cmd, repo_folder)
        except Exception as e:
            logging.debug(e)
            raise Exception('Error while executing fabric scripts, launch CLI with --verbose to see details')

    @staticmethod
    def checkout(args):        
        while not args.git_branch:
            args.git_branch = input("Enter branch name to checkout: ")
        
        step_template_url, step_template_username, \
             step_template_password, \
             step_template_provider_api, \
             step_template_provider_url, \
             step_template_provider_type = SinaraPipeline.get_step_template_repo(args)

        curr_dir = os.getcwd()
        checkout_pipeline_cmd = f"python sinara_pipeline_checkout.py "\
                                        f"--pipeline_dir={curr_dir} "\
                                        f"--git_provider_type={step_template_provider_type} "\
                                        f"--git_provider_api={step_template_provider_api} "\
                                        f"--git_provider_url={step_template_provider_url} "\
                                        f"--git_username={step_template_username} "\
                                        f"--git_password={step_template_password} "\
                                        f"--git_branch={args.git_branch} "
        if args.steps_folder_glob:
            checkout_pipeline_cmd += f"--steps_folder_glob={args.steps_folder_glob}"
        
        try:
            repo_folder = SinaraPipeline.ensure_dataflow_fabric_repo_exists(args)
            SinaraPipeline.call_dataflow_fabric_command(checkout_pipeline_cmd, repo_folder)
        except Exception as e:
            logging.debug(e)
            raise Exception('Error while executing fabric scripts, launch CLI with --verbose to see details')
