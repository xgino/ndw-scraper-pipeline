import pandas as pd

from simple_xml2csv import XML2PARQUET
xml2parquet = XML2PARQUET()

import logging
# Setup  logger
logging.basicConfig(
    filename='./logs/to_parquet.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('to_parquet')

class PROCESS_XML:
    def __init__(self):
        self.openbrige = self.openbrige()
        self.current_status_messages = self.current_status_messages()
        self.incidents = self.incidents()
        self.roadworks = self.roadworks()
        self.matrixsignal = self.matrixsignal()

    def openbrige(self):
        try:
            print("Start converting 'open bridges'")
            logging.info("Start converting 'open bridges'")


            header_xml = 'situation'
            cols = ['overallSeverity', 'situationVersionTime']
            nested_cols = [
                'confidentiality', 'informationStatus', 
                'situationRecordCreationTime', 'situationRecordVersionTime', 'probabilityOfOccurrence',
                'source', 'validityStatus', 
                'overallStartTime', 'overallEndTime', 
                'latitude', 'longitude', 
                'operatorActionStatus', 'complianceOption', 'generalNetworkManagementType',
            ]

            xml2parquet.xml_to_parquet('./preprocess/brugopeningen.xml/brugopeningen.xml', './data/bridge_opening.parquet', header_xml, cols, nested_cols)
            print("Completed 'open Briges'")
            logging.info("Completed 'open Bridges'")
        except Exception as e:
            logging.error(f"Error in 'open bridges' conversion: {e}")


    def current_status_messages(self):
        try:
            logging.info("Start converting 'current status message'")
            print("Start converting 'current status message'")
            header_xml = 'situation'
            cols = ['overallSeverity', 'situationVersionTime']
            nested_cols = [
                'confidentiality', 'informationStatus', 
                'situationRecordCreationReference', 'situationRecordCreationTime', 'situationRecordVersionTime',
                'probabilityOfOccurrence', 'source', 'validityStatus',
                'overallStartTime', 'overallEndTime',
                'startOfPeriod', 'endOfPeriod',
                'delayBand', 'causeDescription', 'causeType',
                'operatorActionStatus', 'mobility', 'subjects',
                'roadworkHindrance', 'roadworkPlanningStatus', 'roadMaintenanceType',
            ]
            xml2parquet.xml_to_parquet('./preprocess/actuele_statusberichten.xml/actuele_statusberichten.xml', './data/current_status_messages.parquet', header_xml, cols, nested_cols)
            print("Completed 'current status message'")
            logging.info("Completed 'current status message'")
        except Exception as e:
            logging.error(f"Error in 'current status message' conversion: {e}")

    def incidents(self):
        try:
            logging.info("Start converting 'incidents'")
            print("Start converting 'incidents'")
            header_xml = 'situation'
            cols = ['overallSeverity', 'situationVersionTime']
            nested_cols = [
                'confidentiality', 'informationStatus', 
                'situationRecordCreationReference', 'situationRecordCreationTime', 'situationRecordObservationTime', 'situationRecordVersionTime', 'situationRecordFirstSupplierVersionTime', 'probabilityOfOccurrence',
                'source', 'validityStatus', 'overallStartTime',
                'latitude', 'longitude',
                'alertCLocationCountryCode', 'alertCLocationTableNumber', 'alertCLocationTableVersion', 'alertCDirection', 'accidentType',
            ]
            xml2parquet.xml_to_parquet('./preprocess/incidents.xml/incidents.xml', './data/incidents.parquet', header_xml, cols, nested_cols)
            print("Completed 'incidents'")
            logging.info("Completed 'incidents'")
        except Exception as e:
            logging.error(f"Error in 'incidents' conversion: {e}")
    
    def roadworks(self):
        try:
            logging.info("Start converting 'roadworks'")
            print("Start converting 'roadworks'")
            header_xml = 'situation'
            cols = ['overallSeverity', 'situationVersionTime', 'source', 'operatorActionStatus', 'mobility']
            nested_cols = [
                'validityStatus', 'overallStartTime', 'overallEndTime',
                'residualRoadWidth', 'delayBand',
                'latitude', 'longitude', 'alertCLocationCountryCode', 'alertCLocationTableNumber', 'alertCLocationTableVersion', 'alertCDirectionCoded', 'alertCLocationName', 'specificLocation', 'offsetDistance',
                'roadworkHindrance', 'roadworkPlanningStatus', 'roadMaintenanceType',
            ]
            xml2parquet.xml_to_parquet('./preprocess/wegwerkzaamheden.xml/wegwerkzaamheden.xml', './data/roadworks.parquet', header_xml, cols, nested_cols)
            print("Completed 'roadworks'")
            logging.info("Completed 'roadworks'")
        except Exception as e:
            logging.error(f"Error in 'roadworks' conversion: {e}")
    
    def matrixsignal(self):
        try:
            logging.info("Start converting 'Matrixsignal'")
            print("Start converting 'Matrixsignal'")
            header_xml = 'event'
            cols = ['ts_event', 'ts_state', 'sign_id']
            nested_cols = [
                'road', 'carriageway', 'lane', 'km'
            ]
            xml2parquet.xml_to_parquet('./preprocess/Matrixsignaalinformatie.xml/Matrixsignaalinformatie.xml', './data/Matrixsignal.parquet', header_xml, cols, nested_cols)
            print("Completed 'Matrixsignal'")
            logging.info("Completed 'Matrixsignal'")
        except Exception as e:
            logging.error(f"Error in 'Matrixsignal' conversion: {e}")

    def roadworks1(self):
        
        print("Start converting 'roadworks'")
        header_xml = 'siteMeasurements'
        cols = ['measurementTimeDefault']
        nested_cols = [
            'travelTimeType', 'travelTime',
            'referenceValueType', 'travelTimeData', 'carriageway', 'situationRecordExtendedApproved',
            'roadworkHindrance', 'roadworkPlanningStatus', 'roadMaintenanceType',
        ]
        xml2parquet.xml_to_parquet('./preprocess/wegwerkzaamheden.xml/wegwerkzaamheden.xml', './data/roadworks.parquet', header_xml, cols, nested_cols)
        print("Completed 'roadworks'")


if __name__ == "__main__":
    print("========= Start converting XML to parquet =========")
    PROCESS_XML()

    print("============ O .O ============")
    print("========= COMPLETED XML TO PARQUET =========")