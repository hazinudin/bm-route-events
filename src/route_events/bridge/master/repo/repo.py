import route_events.bridge.master.repo.bridge_master_pb2 as bridge_master_pb2
import route_events.bridge.master.repo.bridge_master_pb2_grpc as bridge_master_pb2_grpc
import grpc
from route_events.bridge.master.model import BridgeMaster, ARCGIS_STRFTIME
from google.protobuf.json_format import MessageToDict
import pyarrow as pa
from datetime import datetime, timedelta

def grpc_call(fn):
    def wrapper(*args, **kwargs):
        channel = args[0].channel
        kwargs['stub'] = bridge_master_pb2_grpc.BridgeMasterStub(channel)
        return fn(*args, **kwargs)
        
    return wrapper

class BridgeMasterRepo(object):
    def __init__(self, grpc_host:str):
        self.host = grpc_host
        self.channel = grpc.insecure_channel(self.host)

    @staticmethod
    def current_strftime():
        """
        Current strftime with 7 hours offset.
        """
        current_time = datetime.now()
        current_time = current_time - timedelta(hours=7)

        return datetime.strftime(current_time, ARCGIS_STRFTIME)
    
    @grpc_call
    def get_by_bridge_id(self, bridge_id: str, return_count_only=False, **kwargs):
        """
        Load Bridge Master data from GRPC service with Bridge ID query.
        """
        stub = kwargs['stub']

        if bridge_id is not None:
            request = bridge_master_pb2.BridgeIdRequests(bridge_ids=[bridge_id])
            data = stub.GetByID(request)
        else:
            raise ValueError("bridge_id is None.")
        
        output_bridges = self.message_to_bridge(data)

        # Does not use the input schema
        if return_count_only:
            return len(output_bridges)
        
        elif len(output_bridges) > 0:
            return output_bridges[0]
        
        else:
            return None

    @grpc_call
    def get_by_bridge_number(self, bridge_num: str, return_count_only=False, **kwargs):
        """
        Load Bridge Master data from GRPC service with Bridge number query.
        """
        stub = kwargs['stub']

        if bridge_num is not None:
            request = bridge_master_pb2.NumberRequests(number=[bridge_num])
            data = stub.GetByBridgeNumber(request)
        else:
            raise ValueError("bridge_num is None.")
        
        output_bridges = self.message_to_bridge(data)

        # Does not use the input schema
        if return_count_only:
            return len(output_bridges)
        else:
            return output_bridges
    
    @grpc_call
    def get_nearest(self, bridge: BridgeMaster, radius: int, return_count_only=False, **kwargs):
        """
        Load Bridge Master data from GRPC service with spatial radius (in meter) query.
        """
        stub = kwargs['stub']

        filter_geom = bridge.buffer_area_lambert(radius)
        request = bridge_master_pb2.SpatialFilter(geojson=filter_geom, crs=bridge.lambert_wkt)
        data = stub.GetBySpatialFilter(request)

        output_bridges = self.message_to_bridge(data, excluded_bridge=bridge)

        # Does not use the input schema
        if return_count_only:
            return len(output_bridges)
        else:
            return output_bridges
    
    @grpc_call
    def insert(self, bridge: BridgeMaster, **kwargs):
        """
        Insert data through GRPC service.
        """
        stub = kwargs['stub']
        bridge_pb = bridge.as_pb()

        # Set start date
        bridge_pb.attributes.start_date = self.current_strftime()

        bridges = bridge_master_pb2.Bridges(bridges=[bridge_pb])

        results = stub.Insert(bridges)
        
        return results
    
    @grpc_call
    def update(self, bridge: BridgeMaster, **kwargs):
        """
        Update data through GRPC service.
        """
        stub = kwargs['stub']
        bridge_pb = bridge.as_pb()

        # Set start date
        bridge_pb.attributes.start_date = self.current_strftime()

        bridges = bridge_master_pb2.Bridges(bridges=[bridge_pb])

        results = stub.Update(bridges)

        return results
    
    @grpc_call
    def retire(self, bridge: BridgeMaster, **kwargs):
        """
        Retire data through GRPC service.
        """
        stub = kwargs['stub']
        bridge_pb = bridge.as_pb()
        bridges = bridge_master_pb2.Bridges(bridges=[bridge_pb])

        results = stub.Retire(bridges)

        return results
    
    @grpc_call
    def delete(self, oids: list, **kwargs):
        """
        Delete data based on ObjectID through GRPC service.
        """
        stub = kwargs['stub']
        request = bridge_master_pb2.ObjectIdRequests(objectids=oids)

        results = stub.Delete(request)

        return results
    
    def message_to_bridge(self, bridges_pb, excluded_bridge:BridgeMaster=None):
        """
        Deserialize Protocol Buffer message to BridgeMaster object
        """
        output_bridges = list()

        for response_bridge in bridges_pb.bridges:
            data_dict = MessageToDict(response_bridge.attributes, preserving_proto_field_name=True)
            data_dict = {k.upper():v for k,v in data_dict.items()}  # Convert keys to uppercase
            artable = pa.Table.from_pylist([data_dict])

            bm = BridgeMaster(artable)

            if excluded_bridge is None:
                output_bridges.append(bm)
            elif bm.id != excluded_bridge.id:  # If the response bridge is different with the input bridge
                output_bridges.append(bm)

        return output_bridges
