import worker
from http import HTTPStatus


def main():
    a = worker.main()
    return {
        "statusCode": HTTPStatus.OK,
        "body": 'OK'
    }
