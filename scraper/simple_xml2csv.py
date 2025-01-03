import xml.sax
import pandas as pd

class XML2PARQUET:
    def xml_to_parquet(self, xml_file_path, parquet_file_path, header_xml, cols, nested_cols):
        def parse_xml():
            class BridgeOpeningHandler(xml.sax.ContentHandler):
                def __init__(self):
                    self.current_field = None
                    self.current_record = {}
                    self.data = []
                    self.fields = set()
                    self.is_header_info = False
                    self.is_situation_record = False
                    self.current_nested_field = None
                    self.current_nested_value = None

                def startElement(self, name, attrs):
                    if name == header_xml:
                        self.current_record = {header_xml: attrs.get('id', '')}
                        self.fields.add(header_xml)
                    elif name in cols:
                        self.current_field = name
                        self.fields.add(name)
                        self.current_record[self.current_field] = ""
                    elif name in nested_cols:
                        self.current_nested_field = name
                        self.current_nested_value = ""
                        self.fields.add(name)
                    elif self.current_nested_field:
                        self.current_nested_value = ""

                def characters(self, content):
                    if self.current_field:
                        self.current_record[self.current_field] += content.strip()
                    elif self.current_nested_field:
                        self.current_nested_value += content.strip()

                def endElement(self, name):
                    if name in cols:
                        self.current_field = None
                    elif name in nested_cols:
                        self.current_record[name] = self.current_nested_value
                        self.current_nested_field = None
                        self.current_nested_value = ""
                    elif name == header_xml:
                        self.data.append(self.current_record)
                        self.current_record = {}

            handler = BridgeOpeningHandler()
            parser = xml.sax.make_parser()
            parser.setContentHandler(handler)
            parser.parse(xml_file_path)
            return handler.data, sorted(handler.fields, key=lambda f: cols + nested_cols)

        def write_parquet(data, fields):
            df = pd.DataFrame(data, columns=fields)
            df.to_parquet(parquet_file_path, index=False)


        # Define the order of columns
        ordered_fields = cols + nested_cols
        # Parse XML and write to Parquet
        data, fields = parse_xml()
        write_parquet(data, ordered_fields)

