import requests
import xml.etree.ElementTree as ET


def create_lists_from_xml2():
    category_dict = {}
    response = requests.get('https://mobistore.by/integration/icml/icml.xml', stream=True, timeout=200)
    response.raw.decode_content = True

    events = ET.iterparse(response.raw)
    for event, elem in events:
        if elem.tag == 'category':
            category_dict[elem.attrib.get('id')] = {'category_id': elem.attrib.get('id'),
                                                    'category_name': elem.text,
                                                    'parent_category': elem.attrib.get('parentId')}
    return category_dict


def main(args):
    b = create_lists_from_xml2()
    if b:
        return {"body": b}

