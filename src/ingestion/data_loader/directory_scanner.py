from ingestion.log_type_checker import LogTypeChecker
import os
from collections import deque

from .base_scanner import BaseScanner
from .state_manager import StateManager

class DirectoryReader(BaseScanner):

    def __init__(self, base_dir="data/logs", db_path="../../../temp/scanner_state.db"):
        self.base_dir = base_dir
        self.state_manager = StateManager(db_path)
        self.file_queue = deque()

    def _discover_and_enqueue(self):

        """
        Recursively explores the data directory.
        Sorts the subdirectories (day-wise) to keep sequential ingestion.
        """

        print(f" Crawling directory structure starting at: {self.base_dir}")
        
        
        for root, dirs, files in os.walk(self.base_dir):
            
            dirs.sort()
            
            for file in files:
                full_path = os.path.join(root, file)
                self.file_queue.append(full_path)
                
        print(f"Enqueued {len(self.file_queue)} files for processing.")


    def _send_to_next_phase(self, data_line: str):

        """
        Passes processed data forward to your system pipeline.
        """
        type, serial_data = LogTypeChecker.check_log_type(data_line)

        if type == "EMPTY":
            # error handling logic
            pass
        elif type == "JSON":
            # to json parser
            pass
        elif type == "NGINX":
            # to nginx parser
            pass
        elif type == "APPEVOLVE":
            # appevolve parser
            pass
        elif type == "UNKNOWN":
            # agentic parser
            pass
        
        # print(f"{len(data_line)}")


    def scan_for_data(self):

        """
        Main operational pipeline. Loops through the queue sequentially.
        """

        self._discover_and_enqueue()

        while self.file_queue:
            file_path = self.file_queue.popleft()
            
            try:
                stat_info = os.stat(file_path)
                inode = stat_info.st_ino
            except OSError:
                print(f" Skipping inaccessible file: {file_path}")
                continue

            last_offset = self.state_manager.get_offset(inode)
            
            print(f"Processing: {file_path} | Inode: {inode} | Resuming from byte: {last_offset}")

            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                f.seek(last_offset)
                
                while True:
                    
                    current_line_offset = f.tell()
                    line = f.readline()

                    if not line:
                        break

                    
                    if not self._dummy_type_checker(line):
                        # print(f" Type validation failed at byte {current_line_offset}. Checkpoint saved.") 
                        # Save the current line's offset so we attempt it again next runtime
                        self.state_manager.save_state(inode, file_path, current_line_offset)
                        
                        return 

                    self._send_to_next_phase(line)

                self.state_manager.save_state(inode, file_path, f.tell())
                print(f" Finished file: {file_path}")