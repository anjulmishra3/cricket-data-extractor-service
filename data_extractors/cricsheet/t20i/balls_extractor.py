import json
import os
from pymysql.err import IntegrityError
from sqlalchemy import Table, Column, Integer, String, MetaData, Date, Boolean, create_engine


meta = MetaData()
req_directory = os.environ.get("CRICSHEET_T20I_FILES_DIRECTORY")


class CricsheetBall:
    def __init__(self):
        self.engine = create_engine(
            "mysql+pymysql://root:password@localhost:3306/cricsheet"
        )
        self.connection = self.engine.connect()
        self.table = Table(
            "balls",
            meta,
            Column("match_id", Integer),
            Column("batter", String(40), default=""),
            Column("bowler", String(40), default=""),
            Column("non_striker", String(40), default=""),
            Column("batting_team", String(40), default=""),
            Column("fielding_team", String(40), default=""),
            Column("innings_number", Integer),
            Column("over_number", Integer),
            Column("ball_number", Integer),
            Column("delivery_number", Integer),
            Column("batter_runs", Integer),
            Column("extras", Integer),
            Column("extras_type", String(40), default=""),
            Column("total_runs", Integer),
            Column("total_score", Integer),
            Column("total_wickets", Integer),
            Column("wicket", Boolean, default=False),
            Column("wicket_type", String(40), default=""),
            Column("player_out", String(40), default=""),
            Column("wicket_fielder_1", String(40), default=""),
            Column("wicket_fielder_2", String(40), default=""),
        )

    def bulk_insert_matches(self, data):
        try:
            response = self.connection.execute(self.table.insert(), data)
        except IntegrityError as e:
            print(e)
            return

        return response

    def process_json_to_data(self, data, filename):
        match_innings = data.get("innings", {})
        match_balls = []
        innings_tracker = 1
        for innings in match_innings:
            score_tracker = 0
            wickets_tracker = 0
            extras_tracker = 0
            for over in innings["overs"]:
                balls_tracker = 0
                delivery_tracker = 0
                for delivery in over.get("deliveries", []):
                    score_tracker += delivery.get("runs", {}).get("total", 0)
                    wickets_tracker += 1 if delivery.get("wickets") else 0
                    extras_tracker += delivery.get("runs", {}).get("total", 0)
                    balls_tracker += 0 if (delivery.get("extras") and ({"wides", "noballs"}.intersection(set(delivery.get("extras", {}).keys())))) else 1
                    delivery_tracker += 1
                    processed_ball = {
                        "match_id": filename.split(".")[0],
                        "batting_team": innings.get("team"),
                        "fielding_team": data["info"]["teams"][0] if data["info"]["teams"][0] != innings.get("team") else data["info"]["teams"][1],
                        "batter": delivery.get("batter"),
                        "bowler": delivery.get("bowler"),
                        "non_striker": delivery.get("non_striker"),
                        "innings_number": innings_tracker,
                        "over_number": over.get("over"),
                        "ball_number": balls_tracker,
                        "delivery_number": delivery_tracker,
                        "batter_runs": delivery.get("runs", {}).get("batter", 0),
                        "extras": delivery.get("runs", {}).get("extras", 0),
                        "extras_type": [key for key in delivery.get("extras", {}).keys()][0] if delivery.get("extras") else None,
                        "total_runs": delivery.get("runs", {}).get("total", 0),
                        "total_score": score_tracker,
                        "total_wickets": wickets_tracker,
                        "wicket": True if delivery.get("wickets") else False,
                        "wicket_type": delivery.get("wickets", [])[0].get("kind") if delivery.get("wickets") else None,
                        "player_out": delivery.get("wickets", [])[0].get("player_out") if delivery.get("wickets") else None,
                        "wicket_fielder_1": delivery.get("wickets", [])[0].get("player_out") if delivery.get("wickets") else None,
                        "wicket_fielder_2": delivery.get("wickets", [])[0].get("fielders")[0].get("name") if (delivery.get("wickets") and delivery.get("wickets", [])[0].get("fielders")) else None,
                    }
                    match_balls.append(processed_ball)
            innings_tracker += 1

        return match_balls


def open_json_file(filepath):
    try:
        with open(filepath) as f:
            data = json.loads(f.read())
        return data
    except Exception as e:
        print(e, filepath)
        return None


def main(directory):
    cricsheet_match = CricsheetBall()

    for filename in os.listdir(directory):
        file_data = open_json_file(directory + filename)
        if not file_data:
            continue

        try:
            data = cricsheet_match.process_json_to_data(file_data, filename)
        except Exception as e:
            print(filename, e)
            continue

        insertion = cricsheet_match.bulk_insert_matches(data)


main(req_directory)
