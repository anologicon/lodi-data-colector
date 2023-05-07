from requests import get
from repository.data_writer.minio_writer import MinioWriter


if __name__ == "__main__":

    writer = MinioWriter("league-of-data-bronze")

    versions = get("https://ddragon.leagueoflegends.com/api/versions.json").json()
    last_version = versions[0]

    champions_pt_br = get(
        f"http://ddragon.leagueoflegends.com/cdn/{last_version}/data/pt_BR/champion.json"
    ).json()
    items_pt_br = get(
        f"http://ddragon.leagueoflegends.com/cdn/{last_version}/data/pt_BR/item.json"
    ).json()
    spell_summoner_pt_br = get(
        f"http://ddragon.leagueoflegends.com/cdn/{last_version}/data/en_US/summoner.json"
    ).json()

    static_data = {
        "champion_data": champions_pt_br,
        "items_data": items_pt_br,
        "spell_data": spell_summoner_pt_br,
    }

    writer.write(
        f"static_data/version={last_version}/static_data",
        static_data,
    )