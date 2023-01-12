import icml
from http import HTTPStatus


def main():
    return {
        "statusCode": HTTPStatus.OK,
        "body": icml.main()
    }
