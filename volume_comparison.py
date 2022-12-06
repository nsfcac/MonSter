import logging
from datetime import datetime, timedelta
from dotenv import dotenv_values

import pandas as pd
import psycopg2
import pytz
import matplotlib.pyplot as plt

from tsdb.get_records import get_records
from utils.mape import mape
from utils.reconstruction import partition_records, reconstruct, reconstruct_parallel
from utils.deduplication import deduplicate

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z"
)

logger = logging.getLogger("volume_comparison")

TSDB_CONFIG = dotenv_values(".env")

CONNECTION_STRING = f"dbname={TSDB_CONFIG['DBNAME']} user={TSDB_CONFIG['USER']} password={TSDB_CONFIG['PASSWORD']} options='-c search_path=idrac8'"

TABLES = [
    # "rpmreading",
    "systempowerconsumption",
    # "temperaturereading",
]

TIMEDELTA_DAYS = 1


def volume_comparison():
    """Performs volume comparison for different datasets.
    """
    end_date = pytz.utc.localize(datetime.strptime("07/30/2022-00:00", "%m/%d/%Y-%H:%M"))
    start_date = end_date - timedelta(days=TIMEDELTA_DAYS)
    
    print("Start date: ", start_date)
    print("End date: ", end_date)
    
    try:
        with psycopg2.connect(CONNECTION_STRING) as conn:
            for table in TABLES:
                print("Table: ", table)
                original_records = get_records(conn, table, start_date, end_date)
                print("Original records length: ", len(original_records))
                
                deduplicated_records = deduplicate(original_records, formula="cv")
                deduplicated_length_percentage = len(deduplicated_records) / len(original_records) * 100
                print("Deduplicated length: ", len(deduplicated_records))
                print("Deduplicated / Original * 100: ", deduplicated_length_percentage, "%")
                
                partitioned_records = partition_records(deduplicated_records)
                
                reconstructed_records = reconstruct(partitioned_records, start_time=start_date, end_time=end_date)
                reconstructed_length_percentage = len(reconstructed_records) / len(original_records) * 100
                print("Reconstructed length: ", len(reconstructed_records))
                print(f"Reconstructed / Original * 100: {reconstructed_length_percentage}%")
                
                original_df = pd.DataFrame(original_records).sort_values([1, 3, 0])
                recon_df = pd.DataFrame(reconstructed_records).sort_values([1, 3, 0])
                
                count = 0
                mapes = []
                max_node_mape = -1
                max_mape = -1
                for node in range(1, 468):
                    orig_node_df = original_df[original_df[1] == node]
                    recon_node_df = recon_df[recon_df[1] == node]
                    
                    if len(orig_node_df) == len(recon_node_df) and len(orig_node_df) > 0 and len(recon_node_df) > 0:
                        node_recon_orig_mape = mape(orig_node_df[4], recon_node_df[4])
                        if (node_recon_orig_mape > max_mape):
                            max_mape = node_recon_orig_mape
                            max_node_mape = node
                        mapes.append(node_recon_orig_mape)
                        count += 1
                        
                print(f"Orig & Recon node matches: {count}")
                print(f"Orig unique nodes: {len(pd.unique(original_df[1]))}")
                print(f"Recon / Orig * 100: {count / len(pd.unique(original_df[1])) * 100}%")
                print(f"Max MAPE from node {max_node_mape}: {max(mapes)}%")
                print(f"Average MAPE: {sum(mapes) / len(mapes)}%\n")
                
                # length = len(recon_df) if len(recon_df) < len(original_df) else len(original_df)
                
                # recon_orig_mape = mape(original_df[4][:length], recon_df[4][:length])
                # recon_orig_mape = mape(orig_filter_df[4], recon_filter_df[4])
                
                # print("MAPE: ", recon_orig_mape, "%\n")
                
    except Exception as err:
        logger.error("%s", err)
        
        
def query_runtime_comparison():
    """Performs comparison of query runtime for raw and deduplicated reconstructed datasets.
    """
    end_date = pytz.utc.localize(datetime.strptime("07/30/2022-00:00", "%m/%d/%Y-%H:%M"))
    start_date = end_date - timedelta(days=TIMEDELTA_DAYS)
    
    print("Start date: ", start_date)
    print("End date: ", end_date)
    
    try:
        with psycopg2.connect(CONNECTION_STRING) as conn:
            for table in TABLES:
                print("Table: ", table)
                timer_start = datetime.now()
                original_records = get_records(conn, table, start_date, end_date)
                timer_finish = datetime.now() - timer_start
                print(f"Raw runtime: {timer_finish.total_seconds()} seconds")
                
                dedup_table = "deduplicated_" + table
                timer_start = datetime.now()
                deduplicated_records = get_records(conn, dedup_table, start_date, end_date)
                timer_finish = datetime.now() - timer_start
                print(f"Dedup. fetch runtime: {timer_finish.total_seconds()} seconds")
                
                timer_start = datetime.now()
                partitioned_records = partition_records(deduplicated_records)
                # filtered_records = {node: partitioned_records[node] for node in [1, 2]}
                reconstructed_records = reconstruct(partitioned_records, start_time=start_date, end_time=end_date)
                
                original_df = pd.DataFrame(original_records).sort_values([1, 3, 0])
                recon_df = pd.DataFrame(reconstructed_records).sort_values([1, 3, 0])
                
                chosen_node = 202
                # chosen_node = 0
                # for node in range(1, 468):
                #     orig_node_df = original_df[original_df[1] == node]
                #     recon_node_df = recon_df[recon_df[1] == node]
                    
                #     if len(orig_node_df) == len(recon_node_df) and len(orig_node_df) > 0 and len(recon_node_df) > 0:
                #         chosen_node = node
                #         break
                        
                orig_node_df = original_df[original_df[1] == chosen_node].sort_values([1, 3, 0])
                recon_node_df = recon_df[recon_df[1] == chosen_node].sort_values([1, 3, 0])
                
                # orig_node_df = orig_node_df[orig_node_df[3] == "FAN_1"].sort_values([1, 3, 0])
                # recon_node_df = recon_node_df[recon_node_df[3] == "FAN_1"].sort_values([1, 3, 0])
                
                orig_node_df.columns = ["Timestamp", "Node ID", "Source", "FQDD", "Original"]
                recon_node_df.columns = ["Timestamp", "Node ID", "Source", "FQDD", "Reconstructed"]
                
                print(orig_node_df)
                print(recon_node_df)
                
                fig, axs = plt.subplots(figsize=(12, 4))
                
                axs.xaxis.label.set_fontsize(14)
                axs.yaxis.label.set_fontsize(14)
                
                plt.style.use("seaborn-paper")
                
                plt.title('Node 202 System Power Consumption Original vs. Reconstructed Readings', fontdict = {'fontsize' : 16})
                
                orig_node_df.plot.line(x=0, y=4, ax=axs, ylabel="Power Consumption (kWh)", linestyle="-", color="green")
                
                recon_node_df.plot.line(x=0, y=4, ax=axs, ylabel="Power Consumption (kWh)", linestyle=":", color="magenta")
                
                fig.savefig(f"202_{table}_orig_vs_recon_metrics.png")
                
                plt.show()
                
                # reconstructed_records = reconstruct_parallel(partitioned_records, start_time=start_date, end_time=end_date)
                timer_finish = datetime.now() - timer_start
                print(f"Recon. runtime: {timer_finish.total_seconds()} seconds")
                print(f"Recon. length: {len(reconstructed_records)}\n")

    except Exception as err:
        logger.error("%s", err)


if __name__ == '__main__':
    volume_comparison()
    # query_runtime_comparison()
