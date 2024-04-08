from .pipeline import SinaraPipeline

class CommandHandler:
    
    @staticmethod
    def add_command_handlers(root_parser, subject_parser):
        SinaraPipeline.add_command_handlers(root_parser, subject_parser)