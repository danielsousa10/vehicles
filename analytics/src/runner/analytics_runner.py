import logging

from pathlib import Path
from tabulate import tabulate

from service.database import Database

class Runner:
    QUERY_PATH = str(Path(__file__).parents[1]) + "/queries/"

    #Dictionary defining each analysis and corresponding query location
    _queries = {
        'select': QUERY_PATH + 'select.sql',
        'select_by_region': QUERY_PATH + 'select_by_region.sql',
        'select_by_box': QUERY_PATH + 'select_by_box.sql',
        'select_top_region_latest': QUERY_PATH + 'select_top_region_latest.sql',
        'select_regions_cheap_mobile': QUERY_PATH + 'select_regions_cheap_mobile.sql'
    }

    def __init__(self):
        self._dt = Database()
    
    def read_query(self, file_name: str, *args) -> str:
        """Read and format the underlying query"""
        file = Path(file_name)

        if not file.is_file():
            raise FileNotFoundError(f"File {file_name} not found!")
        
        return file.read_text().format(*args)

    def get_result(self, analysis_name: str, *args) -> str:
        """Get results by an analysis name"""
        result = ""

        logging.info(f"Getting analysis *{analysis_name}*...")

        try:
            query = self.read_query(self._queries[analysis_name], *args)

            exe = self._dt.connection.execute(query)

            presult = []
            for row in exe.fetchall():
                presult.append(tuple([str(x) for x in row]))

            result = tabulate(presult, headers=list(exe.keys()), tablefmt='psql', floatfmt=".20f")
        except Exception as e:
            logging.error(f"Failed to get analysis {analysis_name} with error:\n {e}")
        
        return result

    def run_select(self, num_rows: int) -> str:
        return self.get_result("select", num_rows)
    
    def run_select_by_region(self, region: str) -> str:
        return self.get_result("select_by_region", region)
    
    def run_select_by_box(self, box: str) -> str:
        return self.get_result("select_by_box", box)
    
    def run_select_top_region_latest_datasource(self, top: int) -> str:
        return self.get_result("select_top_region_latest", top)

    def select_regions_cheap_mobile(self) -> str:
        return self.get_result("select_regions_cheap_mobile")