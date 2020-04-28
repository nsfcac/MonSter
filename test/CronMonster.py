from helper import parse_config
from crontab import CronTab

def main():
    config = parse_config()

    # Monitoring frequency
    freq = config["frequency"]
    cron = CronTab(user="monster")
    job = cron.new(command='python3 monster.py')
    job.minute.every(freq)
        
        # schedule.every(freq).seconds.do(write_db, client, config, hostlist)

        # # # while 1:
        # # #     schedule.run_pending()
        # # #     time.sleep(freq)

        # for i in range(5):
        #     schedule.run_pending()
        #     time.sleep(freq)

    return 


if __name__ == '__main__':
    main()