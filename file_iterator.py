# CDR BASE FILE
import abc
from datetime import datetime,timedelta
import re
import logging
import os
import traceback
import shutil
import config
import pandas 
import json

from db import write_to_db

class CDRiterator:
    # Static variables

    def __init__(self, seperator=";", min_columns=4, the_infix="", skip_lines=0, last_line = True):
        # Setting default class variables
        self.line_counter = 0  # counting lines valid lines in file
        self.line_count = 0
        self.the_inflix = the_infix
        self.seperator = seperator
        self.last_line = last_line
        self.min_columns = min_columns  # setting the minimum column count before write to DB
        self.skip_lines = skip_lines
        self.retry_lines = 0
        self.current_file = ""
        self.current_file_name = ""
        self.num_errors = 0
        self.bulk_lines = []
        self.retry_handler = None
        self.log_run = logging.getLogger("run")
        self.log_error = logging.getLogger("error")

    def init_payload(self):
        # called for every line to set default values
        # making sure that the mapping is ok
        self.payload = {}  
        #self.payload["[A-No_]"] = str("")
        #self.payload["HBChargeFactor"] = float("0.0")
        #self.payload["ANumberType"] = int("-1")
        #self.payload["dateOfCAMELLeg"] = self.get_date("17540101000000", "%Y%m%d%H%M%S", "Date")
        
        self.payload["TYPE"] = str("")
        #TEST
        self.payload["entry_id"] = int("0") # AUTO IN THE DATABASE
        self.payload["brand_id"] = int("1")
        self.payload["call_event"] = str("")
        self.payload["imsi"] = str("")	
        self.payload["imei"] = str("")	
        self.payload["msisdn"] = str("")
        self.payload["call_event_start_timestamp"] = self.get_date("17540101000000", "%Y%m%d%H%M%S", "Date")
        self.payload["utc_time_offset"] = str("")
        self.payload["call_event_duration"] = int("0")
        self.payload["cause_for_termination"] = int("0")
        self.payload["bearer_service_code"] = str("")
        self.payload["tele_service_code"] = str("")
        self.payload["supplementary_service_code"] = str("")
        self.payload["dialled_digits"] = str("")
        self.payload["connected_number"] = str("")
        self.payload["connected_number_corrected"] = str("")
        self.payload["calling_number"] = str("")
        self.payload["calling_number_corrected"] = str("")
        self.payload["third_party_number"] = str("")
        self.payload["third_party_number_corrected"] = str("")
        self.payload["access_point_name_ni"] = str("")
        self.payload["access_point_name_oi"] = str("")
        self.payload["data_volume_incoming"] = int("0")
        self.payload["data_volume_outgoing"] = int("0")
        self.payload["sgsn_address"] = str("")
        self.payload["ggsn_address"] = str("")
        self.payload["rec_entity_id"] = str("")
        self.payload["call_reference"] = int("0")
        self.payload["charging_id"] = int("0")
        self.payload["charge_amount"] = int("0")
        self.payload["zone_to_id"] = int("0")
        self.payload["zone_from_id"] = int("0")
        self.payload["price"] = float("0")
        #TEST
        self.payload["call_rated"] = bool("False") #TRUE OR FALSE: UNSURE
        self.payload["country_code"] = str("")
        self.payload["plmn"] = str("")
        self.payload["nrtrde_file_id"] = str(self.current_file_name)
        self.payload["inserted"] = self.get_date("17540101000000", "%Y%m%d%H%M%S", "Date")
        #TEST
        self.payload["modified"] = self.get_date("17540101000000", "%Y%m%d%H%M%S", "Date") # AUTO INSERTED IN DATABASE
        self.payload["error_description"] = str("")
        
        
    def run_file(self, file_name):
        self.log_run.info("Process: " + file_name)
        if config.incoming != None:
            self.directory = config.incoming
        self.current_file_name = file_name
        self.current_file = self.directory + "/" + file_name
        modified_date = datetime.fromtimestamp((os.path.getmtime(self.current_file)))
        # Getting the date from the filename as age
        current_file_age = (datetime.now() - modified_date).days
        retry_file_base = file_name
        retry_f = re.search("(.*)_retry_([0-9]+)", file_name)
        retry_num = 1
        if retry_f != None:
            # cheking if there is any files in retry folder with *_retry_* in the name
            # wait at least one day before retry a retry
            self.skip_lines = 0
            if current_file_age < -1:
                return
            retry_num = int(retry_f.group(2)) + 1
            retry_file_base = retry_f.group(1)
        retry_file_name = config.retry + "/" + retry_file_base + "_retry_" + str(retry_num)  # intializing retry file and adding lines that fails
        
        # making dataframe 
        df = pandas.read_csv(self.current_file, dtype=str, skiprows=self.skip_lines, header=None, sep=self.seperator)
        df.dropna(how="all", inplace=True)
            
        if retry_f == None and self.last_line:
            df = df[:-1] # remove last line
        df.fillna('', inplace=True) # empty field for nan
        self.retry_list = []
        for index, row in df.iterrows():
            try:
                self.handle_line(row.values.tolist())
                self.line_counter += 1
            except Exception as e:
                self.num_errors += 1
                self.log_error.error("Error in line, " + self.current_file_name + ": " + str(index + self.skip_lines+1))
                self.log_error.error(traceback.format_exc().splitlines()[-1])
                self.retry_list.append(";".join(row.values.tolist()))
                self.retry_lines += 1
                if config.debug:
                    raise e        
                
        write_to_db(self) # write the rest of the files to DB
        if self.retry_lines != 0:
            with open(retry_file_name, "w+") as self.retry_handler:
                for line in self.retry_list:
                    self.retry_handler.write(line +"\n")
        
        self.log_run.info("Done file: " + file_name + " - Lines: " + str(self.line_counter) + " - errors: " + str(self.num_errors))
        self.move_file()

    def move_file(self, end_location=config.done):
        # Moving file to done folder
        if config.move_files:
            shutil.move(self.current_file, end_location + "/" + self.current_file_name)
            
    def handle_line(self,columns): 
        # Prossesing columns list
        self.init_payload()
        if len(columns) >= self.min_columns:
            if self.handle_columns(columns):
                #self.Check_Payload_len(self.payload) # NEEDS UPDATE
                self.bulk_lines.append(self.payload.copy())
                if len(self.bulk_lines) == config.cdr_bulk_size_limit:
                    write_to_db(self) # Writes config.cdr_bulk_size_limit files to DB

    def get_date(self, full_date, the_format, type,addDuration = False):
        if addDuration:
            converted_date = datetime.strptime(full_date, the_format) + timedelta(seconds=addDuration)
        else:   
            converted_date = datetime.strptime(full_date, the_format)
        if type == "Date":
            return datetime.combine(converted_date.date(),datetime.min.time())
        elif type == "FullDate":
            return converted_date
        elif type == "Time":
            return  datetime.combine(datetime(1754, 1, 1), converted_date.time())
        elif type == "Month":
            return int(datetime.strptime(full_date, the_format).month)
        else:
            return converted_date
      
    def is_number(self, string):
        try:
            float(string)
            return True
        except ValueError:
            return False
        
   
    def Hex_to_Text(self,hex):
        bytes_object = bytes.fromhex(hex)
        text = bytes_object.decode("ASCII")
        return text
    
    def Check_Payload_len(self, payload_):
        if (len(payload_["[A-No_]"])) > 32: 
            raise Exception('A-No_:{} to long for DB, expecting 32!'.format(len(payload_["[A-No_]"])))
        if (len(str(payload_["ANumberType"]))) > 10: 
            raise Exception('ANumberType:{} to long for DB, expecting 10!'.format(len(str(payload_["ANumberType"]))))
        if (len(str(payload_["ANumberPlan"]))) > 10: 
            raise Exception('ANumberPlan:{} to long for DB, expecting 10!'.format(len(str(payload_["ANumberPlan"]))))
        
        if (len(payload_["[B-No_]"])) > 63: 
            raise Exception('B-No_:{} to long for DB, expecting 63!'.format(len(payload_["[B-No_]"])))
        if (len(payload_["[B-No_ Area Code]"])) > 31: 
            raise Exception('B-No_ Area Code:{} to long for DB, expecting 2!'.format(len(payload_["[B-No_ Area Code]"])))
        if (len(str(payload_["BNumberPlan"]))) > 10: 
            raise Exception('BNumberPlan:{} to long for DB, expecting 10!'.format(len(str(payload_["BNumberPlan"]))))
        if (len(str(payload_["BNumberType"]))) > 10: 
            raise Exception('BNumberType:{} to long for DB, expecting 10!'.format(len(str(payload_["BNumberType"]))))
        if (len(str(payload_["BytesIn"]))) > 19: 
            raise Exception('BytesIn:{} to long for DB, expecting 19!'.format(len(str(payload_["BytesIn"]))))
        if (len(str(payload_["BytesOut"]))) > 19: 
            raise Exception('BytesOut:{} to long for DB, expecting 19!'.format(len(str(payload_["BytesOut"]))))
        if (len(payload_["BytesRef"])) > 20: 
            raise Exception('BytesRef:{} to long for DB, expecting 20!'.format(len(payload_["BytesOut"])))
        if (len(payload_["[B-Zone]"])) > 20: 
            raise Exception('B-Zone:{} to long for DB, expecting 20!'.format(len(payload_["[B-Zone]"])))
        
        if (len(payload_["[Call ID]"])) > 64: 
            raise Exception('Call ID:{} to long for DB, expecting 64!'.format(len(payload_["[Call ID]"])))
        if (len(str(payload_["[Cause for Termination]"]))) > 10: 
            raise Exception('Cause for Termination:{} to long for DB, expecting 10!'.format(len(str(payload_["[Cause for Termination]"]))))
        if (len(str(payload_["callReferenceNumber"]))) > 19: 
            raise Exception('callReferenceNumber:{} to long for DB, expecting 19!'.format(len(str(payload_["callReferenceNumber"]))))
        if (len(payload_["[Cell ID]"])) > 10: 
            raise Exception('Cell ID:{} to long for DB, expecting 10!'.format(len(payload_["[Cell ID]"])))
        if (len(payload_["changeFlags"])) > 2: 
            raise Exception('changeFlags:{} to long for DB, expecting 2!'.format(len(payload_["changeFlags"])))
        if (len(payload_["[Circuit IN]"])) > 20: 
            raise Exception('Circuit IN:{} to long for DB, expecting 20!'.format(len(payload_["[Circuit IN]"])))
        if (len(payload_["[Circuit OUT]"])) > 20: 
            raise Exception('Circuit OUT:{} to long for DB, expecting 20!'.format(len(payload_["[Circuit OUT]"])))
        if (len(payload_["Currency"])) > 3: 
            raise Exception('Currency:{} to long for DB, expecting 3!'.format(len(payload_["Currency"])))
        
        if (len(payload_["DialledDigits"])) > 63: 
            raise Exception('DialledDigits:{} to long for DB, expecting 63!'.format(len(payload_["Currency"])))
        if (len(payload_["Direction"])) > 1: 
            raise Exception('Direction:{} to long for DB, expecting 1!'.format(len(payload_["Currency"])))
        if (payload_["Duration"]) > 1e+18: 
            raise Exception('Duration:{} to long for DB, expecting 1e+17!'.format(payload_["Duration"]))
        if (payload_["durationOfCAMELLeg"]) > 1e+18: 
            raise Exception('durationOfCAMELLeg:{} to long for DB, expecting 1e+17!'.format(payload_["durationOfCAMELLeg"]))
        
        if (len(payload_["EDRW_ErrClass"])) > 10: 
            raise Exception('EDRW_ErrClass:{} to long for DB, expecting 10!'.format(len(payload_["EDRW_ErrClass"])))
        if (len(payload_["EDRW_ErrMessage"])) > 250: 
            raise Exception('EDRW_ErrMessage:{} to long for DB, expecting 1!'.format(len(payload_["EDRW_ErrMessage"])))
        if (len(payload_["EDRW_ErrName"])) > 1: 
            raise Exception('EDRW_ErrName:{} to long for DB, expecting 128!'.format(len(payload_["EDRW_ErrName"])))
        if (payload_["ExchangeRate"]) > 1e+18: 
            raise Exception('ExchangeRate:{} to long for DB, expecting 1e+17!'.format(payload_["ExchangeRate"]))
        
        if (len(payload_["[File ID]"])) > 250: 
            raise Exception('File ID:{} to long for DB, expecting 250!'.format(len(payload_["[File ID]"])))
        
        if (len(str(payload_["HBCallIsRated"]))) > 3: 
            raise Exception('HBCallIsRated:{} to long for DB, expecting 3!'.format(len(str(payload_["HBCallIsRated"]))))
        if (payload_["HBChargeFactor"]) > 1e+18: 
            raise Exception('HBChargeFactor:{} to long for DB, expecting 1e+17!'.format(payload_["HBChargeFactor"]))
        if (payload_["HBPrice"]) > 1e+18: 
            raise Exception('HBPrice:{} to long for DB, expecting 1e+17!'.format(payload_["HBPrice"]))
        if (len(str(payload_["HBReadyForBufferTransfer"]))) > 3: 
            raise Exception('HBReadyForBufferTransfer:{} to long for DB, expecting 3!'.format(len(str(payload_["HBReadyForBufferTransfer"]))))
        if (len(str(payload_["HBTransferToBuffer"]))) > 10: 
            raise Exception('HBTransferToBuffer:{} to long for DB, expecting 10!'.format(len(str(payload_["HBTransferToBuffer"]))))
        
        if (len(payload_["IMEI"])) > 16: 
            raise Exception('IMEI:{} to long for DB, expecting 16!'.format(len(payload_["IMEI"])))
        if (len(payload_["IMSI"])) > 15: 
            raise Exception('IMSI:{} to long for DB, expecting 15!'.format(len(payload_["IMSI"])))
        if (len(str(payload_["IMSIAddedByEDRW"]))) > 3: 
            raise Exception('IMSIAddedByEDRW:{} to long for DB, expecting 3!'.format(len(str(payload_["IMSIAddedByEDRW"]))))
        if (payload_["IncommingPrice"]) > 1e+18: 
            raise Exception('IncommingPrice:{} to long for DB, expecting 1e+17!'.format(payload_["IncommingPrice"]))
        if (len(str(payload_["inServiceKey"]))) > 10: 
            raise Exception('inServiceKey:{} to long for DB, expecting 10!'.format(len(str(payload_["inServiceKey"]))))
        if (len(payload_["InTransPar"])) > 100: 
            raise Exception('InTransPar:{} to long for DB, expecting 100!'.format(len(payload_["InTransPar"])))
        if (len(str(payload_["inFlag"]))) > 19: 
            raise Exception('inFlag:{} to long for DB, expecting 19!'.format(len(str(payload_["inFlag"]))))
        
        if (len(payload_["levelOfCAMELService"])) > 20: 
            raise Exception('levelOfCAMELService:{} to long for DB, expecting 20!'.format(len(payload_["levelOfCAMELService"])))
        if (len(payload_["[Location Area]"])) > 10: 
            raise Exception('Location Area:{} to long for DB, expecting 10!'.format(len(payload_["[Location Area]"])))
        
        
        if (len(payload_["mcrDestinationNumber"])) > 150: 
            raise Exception('mcrDestinationNumber:{} to long for DB, expecting 150!'.format(len(payload_["mcrDestinationNumber"])))
        if (len(str(payload_["mcrDNANumberPlan"]))) > 10: 
            raise Exception('mcrDNANumberPlan:{} to long for DB, expecting 10!'.format(len(str(payload_["mcrDNANumberPlan"]))))
        if (len(str(payload_["mcrDNANumberType"]))) > 10: 
            raise Exception('mcrDNANumberType:{} to long for DB, expecting 10!'.format(len(str(payload_["mcrDNANumberType"]))))
        if (len(str(payload_["MonthNo"]))) > 10: 
            raise Exception('MonthNo:{} to long for DB, expecting 10!'.format(len(str(payload_["MonthNo"]))))
        if (len(payload_["MSCCalltransctionType"])) > 30: 
            raise Exception('MSCCalltransctionType:{} to long for DB, expecting 30!'.format(len(payload_["MSCCalltransctionType"])))
        if (len(payload_["MSCID"])) > 20: 
            raise Exception('MSCID:{} to long for DB, expecting 20!'.format(len(payload_["MSCID"])))
        if (len(str(payload_["multipleSMTransfer"]))) > 10: 
            raise Exception('multipleSMTransfer:{} to long for DB, expecting 10!'.format(len(str(payload_["multipleSMTransfer"]))))
        
        
        if (len(str(payload_["[Net-To-Net Candidate]"]))) > 3: 
            raise Exception('Net-To-Net Candidate:{} to long for DB, expecting 3!'.format(len(str(payload_["Net-To-Net Candidate"]))))
        if (payload_["[No_ of Minutes]"]) > 1e+18: 
            raise Exception('No_ of Minutes:{} to long for DB, expecting 1e+17!'.format(payload_["No_ of Minutes"]))
        if (len(str(payload_["NumberOfDecimalPlaces"]))) > 10: 
            raise Exception('NumberOfDecimalPlaces:{} to long for DB, expecting 10!'.format(len(str(payload_["NumberOfDecimalPlaces"]))))
        if (len(payload_["[Operator B-No]"])) > 10: 
            raise Exception('Operator B-No:{} to long for DB, expecting 10!'.format(len(payload_["Operator B-No"])))
        if (len(payload_["OperatorPhysicalIN"])) > 10: 
            raise Exception('OperatorPhysicalIN:{} to long for DB, expecting 10!'.format(len(payload_["OperatorPhysicalIN"])))
        if (len(payload_["Owner"])) > 20: 
            raise Exception('Owner:{} to long for DB, expecting 20!'.format(len(payload_["Owner"])))
        if (len(payload_["OwnerType"])) > 10: 
            raise Exception('OwnerType:{} to long for DB, expecting 10!'.format(len(payload_["OwnerType"])))
        
        if (len(payload_["PhysicalTerminatingNetwork"])) > 50: 
            raise Exception('PhysicalTerminatingNetwork:{} to long for DB, expecting 50!'.format(len(payload_["PhysicalTerminatingNetwork"])))
        if (len(str(payload_["PrePaidCustomer"]))) > 10: 
            raise Exception('PrePaidCustomer:{} to long for DB, expecting 10!'.format(len(str(payload_["PrePaidCustomer"]))))
        if (len(str(payload_["[Prerated Zone Info]"]))) > 3: 
            raise Exception('Prerated Zone Info:{} to long for DB, expecting 3!'.format(len(str(payload_["[Prerated Zone Info]"]))))
        if (len(payload_["[Product Area]"])) > 1: 
            raise Exception('Product Area:{} to long for DB, expecting 1!'.format(len(payload_["[Product Area]"])))
        
        if (len(str(payload_["[Reason for Termination]"]))) > 10: 
            raise Exception('Reason for Termination:{} to long for DB, expecting 10!'.format(len(str(payload_["[Reason for Termination]"]))))
        
        if (len(str(payload_["sCANumberPlan"]))) > 10: 
            raise Exception('sCANumberPlan:{} to long for DB, expecting 10!'.format(len(str(payload_["sCANumberPlan"]))))
        if (len(str(payload_["sCANumberType"]))) > 10: 
            raise Exception('sCANumberType:{} to long for DB, expecting 10!'.format(len(str(payload_["sCANumberType"]))))
        if (len(payload_["servedMSRN"])) > 100: 
            raise Exception('servedMSRN:{} to long for DB, expecting 100!'.format(len(payload_["servedMSRN"])))
        if (len(payload_["servedSubscriberLocation"])) > 100: 
            raise Exception('servedSubscriberLocation:{} to long for DB, expecting 100!'.format(len(payload_["servedSubscriberLocation"])))
        if (len(payload_["[Service Code]"])) > 20: 
            raise Exception('Service Code:{} to long for DB, expecting 20!'.format(len(payload_["[Service Code]"])))
        if (len(payload_["serviceCentreAddress"])) > 32: 
            raise Exception('serviceCentreAddress:{} to long for DB, expecting 32!'.format(len(payload_["serviceCentreAddress"])))
        if (len(payload_["SlaveANo"])) > 32: 
            raise Exception('SlaveANo:{} to long for DB, expecting 32!'.format(len(payload_["SlaveANo"])))
        if (len(payload_["SlaveIMSI"])) > 15: 
            raise Exception('SlaveIMSI:{} to long for DB, expecting 15!'.format(len(payload_["SlaveIMSI"])))
        if (len(str(payload_["sMTransmissionResult"]))) > 10: 
            raise Exception('sMTransmissionResult:{} to long for DB, expecting 10!'.format(len(str(payload_["sMTransmissionResult"]))))
        if (len(payload_["SourceType"])) > 10: 
            raise Exception('SourceType:{} to long for DB, expecting 10!'.format(len(payload_["SourceType"])))
        if (len(payload_["speechCode"])) > 2: 
            raise Exception('speechCode:{} to long for DB, expecting 2!'.format(len(payload_["speechCode"])))
        if (len(payload_["[SS Code]"])) > 106: 
            raise Exception('SS Code:{} to long for DB, expecting 106!'.format(len(payload_["[SS Code]"])))
        if (len(str(payload_["sSLOriginIndicator"]))) > 10: 
            raise Exception('sSLOriginIndicator:{} to long for DB, expecting 10!'.format(len(str(payload_["sSLOriginIndicator"]))))
        if (len(payload_["[Switch ID]"])) > 10: 
            raise Exception('Switch ID:{} to long for DB, expecting 10!'.format(len(payload_["[Switch ID]"])))
        if (len(str(payload_["systemType"]))) > 10: 
            raise Exception('systemType:{} to long for DB, expecting 10!'.format(len(str(payload_["systemType"]))))
        
        
        if (len(payload_["[Tariff Area]"])) > 20: 
            raise Exception('Tariff Area:{} to long for DB, expecting 20!'.format(len(payload_["[Tariff Area]"])))
        if (payload_["Tax"]) > 1e+18: 
            raise Exception('Tax:{} to long for DB, expecting 1e+17!'.format(payload_["Tax"]))
        if (len(payload_["thirdParty"])) > 100: 
            raise Exception('thirdParty:{} to long for DB, expecting 100!'.format(len(payload_["thirdParty"])))
        if (len(str(payload_["TOPNumberPlan"]))) > 10: 
            raise Exception('TOPNumberPlan:{} to long for DB, expecting 10!'.format(len(str(payload_["TOPNumberPlan"]))))
        if (len(str(payload_["TOPNumberType"]))) > 10: 
            raise Exception('TOPNumberType:{} to long for DB, expecting 10!'.format(len(str(payload_["TOPNumberType"]))))
        if (len(str(payload_["tPNumberPlan"]))) > 10: 
            raise Exception('tPNumberPlan:{} to long for DB, expecting 10!'.format(len(str(payload_["tPNumberPlan"]))))
        if (len(str(payload_["tPNumberType"]))) > 10: 
            raise Exception('tPNumberType:{} to long for DB, expecting 10!'.format(len(str(payload_["tPNumberType"]))))
        if (len(payload_["TranslatedOtherParty"])) > 32: 
            print(payload_["TranslatedOtherParty"])
            raise Exception('TranslatedOtherParty:{} to long for DB, expecting 32!'.format(len(payload_["TranslatedOtherParty"])))
    
    @abc.abstractmethod
    def handle_columns(self, columns):
        """
        Must return a map of values to send to api
        On error this map must contain an "error" field
        with the actual error as value in the map
        returns True if to be sendt to Moflix
        """
        return

    def infix(self):
        """
        What the file starts with determines the file type.
        """
        return self.the_inflix

    @staticmethod
    def find_subclass_to_file(file_name):
        """
        Find right subclass, based on filename"
        """
        subclasses = CDRiterator.__subclasses__()
        return next(filter(lambda c: c().infix() in file_name, subclasses), None)

    @staticmethod
    def to_treatment(f):
        """
        Finding the right subclass to use for the file.
        Running a OtherFiles if not exists
        """
        class_init = CDRiterator.find_subclass_to_file(f)
        the_class = OtherFiles() if class_init == None else class_init()
        #config.conn = db_conn
        the_class.run_file(f)

class OtherFiles(CDRiterator):
    def __init__(self):
        super().__init__(the_infix="GENERIC")

    def run_file(self, file_name):
        self.log_run.info("Generic file: " + file_name)
        
        if config.incoming != None:
            self.directory = config.incoming
        self.current_file_name = file_name
        self.current_file = self.directory + "/" + file_name
        self.move_file(end_location=config.generic)
