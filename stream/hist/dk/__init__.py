import os 

from logging.config import dictConfig

__all__ = ["DKHistBarsStream"]
__version__ = 0.1

dictConfig({
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {
                    'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
                },
            },
            'handlers': {
                'stream': {
                    'level': 'DEBUG',
                    'formatter': 'standard',
                    'class': 'logging.StreamHandler',
                },
                'file': {
                    'level': 'DEBUG',
                    'formatter':'standard',
                    'class': 'logging.FileHandler',
                    'filename': f"{os.getcwd()}/src/stream/logging/dk_bars_stream.log",
                }
            },
            'loggers': {
                'dk_bars': {
                    'handlers': ['stream', 'file'],
                    'level': 'DEBUG',
                    'propagate': True
                },
            }
        })