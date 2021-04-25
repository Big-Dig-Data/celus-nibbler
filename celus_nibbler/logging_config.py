DEFAULT_LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'},
    },
    'handlers': {
        'default': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout',  # Default is stderr
        },
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout',  # Default is stderr
        },
        # 'file': {
        #     'class': 'logging.handlers.RotatingFileHandler',
        #     'level': 'INFO',
        #     'formatter': 'standard',
        #     'filename': 'logconfig.log',
        #     'maxBytes': 1024,
        #     'backupCount': 3,
        #     'stream': 'ext://sys.stdout',  # Default is stderr
        # },
    },
    'loggers': {
        '': {'handlers': ['default'], 'level': 'WARNING', 'propagate': False},  # root logger
        '__main__': {  # if __name__ == '__main__'
            'handlers': ['default'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}
