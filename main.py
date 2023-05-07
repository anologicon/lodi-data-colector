import logging
from repository.league_of_legends_repository import LeagueOfLegendsRepository
from service.base_request_service import BaseRequestService
from service.api.api_service import APIService
from repository.data_writer.minio_writer import MinioWriter
from concurrent import futures
import argparse

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CHUNK_SIZE = 20

def main(args, api_service):

    writer = MinioWriter("league-of-data-bronze")
    summoners_data_list = api_service.fetch_summoner_data(limit=args.limit)
    with futures.ThreadPoolExecutor() as executor:
        executor.submit(api_service.fetch_summoner_mastery, summoners_data_list, writer)
        future_summoners_data_list = executor.submit(
            api_service.fetch_summoner_match, summoners_data_list, writer, args.max_match
        )
        summoners_data_list = api_service.filter_unique_match_id(future_summoners_data_list.result())
        executor.submit(
            api_service.fetch_match_detail,
            summoners_data_list,
            writer,
        )
        executor.submit(
            api_service.fetch_match_timeline,
            summoners_data_list,
            writer,
        )


def __parse_args_setup():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--limit", type=int, help="Change limit of players", default=200)
    parser.add_argument("-mm", "--max-match", type=int, help="Set max matches for each player", default=10)
    args = parser.parse_args()
    return args


def __bootstrap_app(args):
    api_service = APIService(
        LeagueOfLegendsRepository(BaseRequestService()), args.max_match, CHUNK_SIZE
    )
    return api_service


if __name__ == "__main__":
    print("Challengers ETL - Max 200")
    args = __parse_args_setup()
    api_service = __bootstrap_app(args)
    main(args, api_service)
