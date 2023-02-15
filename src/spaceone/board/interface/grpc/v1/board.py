from spaceone.api.board.v1 import board_pb2, board_pb2_grpc
from spaceone.core.pygrpc import BaseAPI
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter


class Board(BaseAPI, board_pb2_grpc.BoardServicer):
    # set_global_textmap(B3Format())
    # propagator = propagate.get_global_textmap()
    pb2 = board_pb2
    pb2_grpc = board_pb2_grpc
    resource = Resource(attributes={
        SERVICE_NAME: "board"
    })
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://otel-poc-collector.otel.svc.cluster.local:4317"))
    # processor = BatchSpanProcessor(ConsoleSpanExporter())
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer(__name__)

    def list(self, request, context):

        with self.tracer.start_as_current_span("BoardService.list") as span:
            print("trace_id: ", span.get_span_context().trace_id)
            params, metadata = self.parse_request(request, context)

            with self.locator.get_service('BoardService', metadata) as board_service:
                board_vos, total_count = board_service.list(params)
                return self.locator.get_info('BoardsInfo',
                                             board_vos,
                                             total_count,
                                             minimal=self.get_minimal(params))

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BoardService', metadata) as board_service:
            return self.locator.get_info('BoardInfo', board_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BoardService', metadata) as board_service:
            return self.locator.get_info('BoardInfo', board_service.update(params))

    def set_categories(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BoardService', metadata) as board_service:
            return self.locator.get_info('BoardInfo', board_service.set_categories(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BoardService', metadata) as board_service:
            board_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BoardService', metadata) as board_service:
            return self.locator.get_info('BoardInfo', board_service.get(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BoardService', metadata) as board_service:
            return self.locator.get_info('BoardInfo', board_service.stat(params))
