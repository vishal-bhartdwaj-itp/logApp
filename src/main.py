from ingestion.scanners.directory_scanner import DirectoryReader

from pipeline.parser_worker import ParserWorker

from observability.metrics import start_metrics_server

from observability.logging_config import setup_logger


logger = setup_logger()


def main():

    logger.info("Starting logApp")

    start_metrics_server()

    worker = ParserWorker(worker_count=4)

    worker.start()

    scanner = DirectoryReader()

    scanner.scan_for_data()


if __name__ == "__main__":
    main()