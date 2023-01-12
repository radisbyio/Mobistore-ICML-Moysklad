import worker
from http import HTTPStatus


def main():
    worker.main()
    return {
        "statusCode": HTTPStatus.OK,
        "body": 'OK'
    }
