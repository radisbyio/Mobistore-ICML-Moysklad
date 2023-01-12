import worker
from http import HTTPStatus


def main():
    a = worker.main()
    return {
        'body': {
            'text': 'OK'
        }
    }
