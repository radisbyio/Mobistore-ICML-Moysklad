import icml
from http import HTTPStatus


def main():
    icml.main()
    return {
        "statusCode": HTTPStatus.OK,
        "body": 'OK'
    }
