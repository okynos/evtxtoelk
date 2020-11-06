import contextlib
import mmap
import traceback
import json
import argparse
from collections import OrderedDict
from datetime import datetime

from Evtx.Evtx import FileHeader
from Evtx.Views import evtx_file_xml_view
from elasticsearch import Elasticsearch, helpers
import xmltodict
import sys


class EvtxToElk:
    @staticmethod
    def bulk_to_elasticsearch(es, bulk_queue):
        try:
            helpers.bulk(es, bulk_queue)
            return True
        except:
            print(traceback.print_exc())
            return False

    @staticmethod
    def evtx_lines(filename):
        events = []
        with open(filename) as infile:
            with contextlib.closing(mmap.mmap(infile.fileno(), 0, access=mmap.ACCESS_READ)) as buf:
                fh = FileHeader(buf, 0x0)
                data = ""
                for xml, record in evtx_file_xml_view(fh):
                    try:
                        contains_event_data = False
                        log_line = xmltodict.parse(xml)

                        # Format the date field
                        date = log_line.get("Event").get("System").get("TimeCreated").get("@SystemTime")
                        if "." not in str(date):
                            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                        else:
                            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
                        log_line['@timestamp'] = str(date.isoformat())
                        log_line["Event"]["System"]["TimeCreated"]["@SystemTime"] = str(date.isoformat())

                        # Process the data field to be searchable
                        data = ""
                        if log_line.get("Event") is not None:
                            data = log_line.get("Event")
                            if log_line.get("Event").get("EventData") is not None:
                                data = log_line.get("Event").get("EventData")
                                if log_line.get("Event").get("EventData").get("Data") is not None:
                                    data = log_line.get("Event").get("EventData").get("Data")
                                    if isinstance(data, list):
                                        contains_event_data = True
                                        data_vals = {}
                                        for dataitem in data:
                                            try:
                                                if dataitem.get("@Name") is not None:
                                                    data_vals[str(dataitem.get("@Name"))] = str(
                                                        str(dataitem.get("#text")))
                                            except:
                                                pass
                                        log_line["Event"]["EventData"]["Data"] = data_vals
                                    else:
                                        if isinstance(data, OrderedDict):
                                            log_line["Event"]["EventData"]["RawData"] = json.dumps(data)
                                        else:
                                            log_line["Event"]["EventData"]["RawData"] = str(data)
                                        del log_line["Event"]["EventData"]["Data"]
                                else:
                                    if isinstance(data, OrderedDict):
                                        log_line["Event"]["RawData"] = json.dumps(data)
                                    else:
                                        log_line["Event"]["RawData"] = str(data)
                                    del log_line["Event"]["EventData"]
                            else:
                                if isinstance(data, OrderedDict):
                                    log_line = dict(data)
                                else:
                                    log_line["RawData"] = str(data)
                                    del log_line["Event"]
                        else:
                            pass

                        events.append( json.loads(json.dumps(log_line)) )

                    except:
                        print("***********")
                        print("Parsing Exception")
                        print(traceback.print_exc())
                        print(json.dumps(log_line, indent=2))
                        print("***********")
        return events


    @staticmethod
    def evtx_to_elk(filename, elk_ip, elk_index="hostlogs", bulk_queue_len_threshold=500, metadata={}):
        bulk_queue = []
        es = Elasticsearch([elk_ip])

        for event in EvtxToElk.evtx_lines(filename):
            # Insert data into queue
            #event_record.update({
            #    "_index": elk_index,
            #    "_type": elk_index,
            #    "metadata": metadata
            #})
            #bulk_queue.append(event_record)
            event["_index"] = elk_index
            event["_type"] = elk_index
            event["meta"] = metadata
            bulk_queue.append(event)

            #bulk_queue.append({
            #    "_index": elk_index,
            #    "_type": elk_index,
            #    "body": json.loads(json.dumps(log_line)),
            #    "metadata": metadata
            #})

            if len(bulk_queue) == bulk_queue_len_threshold:
                print('Bulkingrecords to ES: ' + str(len(bulk_queue)))
                # start parallel bulking to ElasticSearch, default 500 chunks;
                if EvtxToElk.bulk_to_elasticsearch(es, bulk_queue):
                    bulk_queue = []
                else:
                    print('Failed to bulk data to Elasticsearch')
                    sys.exit(1)

            # Check for any remaining records in the bulk queue
            if len(bulk_queue) > 0:
                print('Bulking final set of records to ES: ' + str(len(bulk_queue)))
                if EvtxToElk.bulk_to_elasticsearch(es, bulk_queue):
                    bulk_queue = []
                else:
                    print('Failed to bulk data to Elasticsearch')
                    sys.exit(1)

    @staticmethod
    def evtx_to_json(filename, output_file, debug = False):
        ctr = 0
        for event in EvtxToElk.evtx_lines(filename):
            if debug:
                print(json.dumps(event))
            # Write event line to JSON file
            with open(output_file, "a") as output:
                output.write(json.dumps(event))
            ctr += 1
        print(str(ctr) + ' events exported to ' + output_file)


if __name__ == "__main__":
    # Create argument parser
    parser = argparse.ArgumentParser()
    # Add arguments
    parser.add_argument('evtxfile', help="Evtx file to parse.")
    parser.add_argument('output', default="localhost", help="IP (and port) of ELK instance or .json filename to local export.")
    parser.add_argument('-i', default="hostlogs", help="ELK index to load data into.")
    parser.add_argument('-s', default=500, help="Size of queue.")
    parser.add_argument('-meta', default={}, type=json.loads, help="Metadata to add to records.")
    parser.add_argument('-d', default=False, help="Debug JSON output process.")
    # Parse arguments and call evtx to elk class
    args = parser.parse_args()
    if '.json' in args.output:
        EvtxToElk.evtx_to_json(args.evtxfile, args.output, args.d)
    else:
        EvtxToElk.evtx_to_elk(args.evtxfile, args.output, elk_index=args.i, bulk_queue_len_threshold=int(args.s), metadata=args.meta)
