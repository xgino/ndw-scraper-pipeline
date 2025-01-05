import os
import pandas as pd
import xml.etree.ElementTree as ET
from tqdm import tqdm


class MultiNestedXMLToParquet:
    def parse_xml_to_dict(self, xml_file_path):
        """
        Parse the XML file incrementally into a list of dictionaries with a flat structure,
        consolidating data for nested and repeating nodes.
        """
        def remove_namespace(tag):
            if not tag:
                return ""
            return tag.split('}', 1)[-1] if '}' in tag else tag

        def flatten_element(element, parent_key=""):
            items = {}
            for child in element:
                child_key = f"{parent_key}_{remove_namespace(child.tag)}" if parent_key else remove_namespace(child.tag)
                items.update(flatten_element(child, child_key))
            if element.text and element.text.strip():
                items[parent_key] = element.text.strip()
            items.update({f"{parent_key}_{remove_namespace(k)}": v for k, v in element.attrib.items()})
            return items

        context = ET.iterparse(xml_file_path, events=("start", "end"))
        context = iter(context)
        _, root = next(context)

        record = {}  # Temporary storage for records
        for event, element in tqdm(context, desc="Processing XML", unit="element"):
            if event == "start":
                continue

            tag = remove_namespace(element.tag)

            if tag == 'event':
                record = flatten_element(element)
                yield record

            root.clear()

    def convert_to_parquet(self, data_generator, output_file):
        chunk_size = 10000
        temp_files = []
        chunks = []

        for idx, record in enumerate(data_generator):
            if record:
                chunks.append(record)

            # Periodically process and write chunks
            if (idx + 1) % chunk_size == 0 and chunks:
                chunk_df = pd.DataFrame(chunks)

                # Remove duplicates within the chunk
                #chunk_df = chunk_df.drop_duplicates()

                temp_file = f"temp_chunk_{len(temp_files)}.parquet"
                temp_files.append(temp_file)
                chunk_df.to_parquet(temp_file, engine="pyarrow", index=False)
                chunks = []  # Reset chunk storage

        # Handle remaining data
        if chunks:
            chunk_df = pd.DataFrame(chunks)
            temp_file = f"temp_chunk_{len(temp_files)}.parquet"
            temp_files.append(temp_file)
            chunk_df.to_parquet(temp_file, engine="pyarrow", index=False)

        # Combine all temp files into a single Parquet file
        combined_df = pd.concat([pd.read_parquet(file) for file in temp_files], ignore_index=True)

        # Drop duplicate rows
        combined_df = combined_df.drop_duplicates()

        # Save final Parquet file
        combined_df.to_parquet(output_file, engine="pyarrow", index=False)

        # Clean up temporary files
        for temp_file in temp_files:
            os.remove(temp_file)

        print(f"Parquet file saved to: {output_file}")


if __name__ == "__main__":
    # xml_file_path = "wegwerkzaamheden.xml"
    # output_file = "wegwerkzaamheden.parquet"
    xml_file_path = "Matrixsignaalinformatie.xml"
    output_file = "Matrixsignaalinformatie.parquet"

    converter = MultiNestedXMLToParquet()
    parsed_data = converter.parse_xml_to_dict(xml_file_path)
    converter.convert_to_parquet(parsed_data, output_file)
