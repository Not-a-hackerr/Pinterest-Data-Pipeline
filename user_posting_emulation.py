import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import yaml
import json
import sqlalchemy
from sqlalchemy import text

random.seed(100)


class AWSDBConnector:
    def read_db_creds(self):
        ''''
        This method reads the yaml file that holds all the credentials for the pgadmin connection and the AWS RDS connection
        '''
        with open('db_creds.yaml') as f:
            data_base_creds = yaml.load(f, Loader=yaml.FullLoader)
        return data_base_creds
   
    def create_db_connector(self):
        read_db_dict = self.read_db_creds()
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{read_db_dict['USER']}:{read_db_dict['PASSWORD']}@{read_db_dict['HOST']}:{read_db_dict['PORT']}/{read_db_dict['DATABASE']}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            # print(pin_result)
            # print(geo_result)
            # print(user_result)

            pin_payload = json.dumps({
                "records": [
                    {
                    "value": {
                        "index": pin_result["index"], 
                        "unique_id": pin_result["unique_id"], 
                        "title": pin_result["title"], 
                        "description": pin_result["description"],
                        "poster_name": pin_result["poster_name"],
                        "follower_count": pin_result["follower_count"],
                        "tag_list": pin_result["tag_list"],
                        "is_image_or_video": pin_result["is_image_or_video"],
                        "image_src": pin_result["image_src"],
                        "downloaded": pin_result["downloaded"],
                        "save_location": pin_result["save_location"],
                        "category": pin_result["category"]}
                    }
                ]
            })

            geo_payload = json.dumps({
                "records": [
                    {
                    "value": {
                        "ind": geo_result["ind"],
                        "timestamp": str(geo_result["timestamp"]),
                        "latitude": geo_result["latitude"],
                        "longitude": geo_result["longitude"],
                        "country": geo_result["country"]}
                    }
                ]
            })
            
            user_payload = json.dumps({
                "records": [
                    {
                    "value": {
                        "ind": user_result["ind"],
                        "first_name": user_result["first_name"],
                        "last_name": user_result["last_name"],
                        "date_joined": str(user_result["date_joined"])}
                    }
                ]
            })
            
            response = requests.request("POST", new_connector.read_db_creds()['invoke_url_geo'], headers=new_connector.read_db_creds()['headers'], data=geo_payload)
            response = requests.request("POST",  new_connector.read_db_creds()['invoke_url_pin'], headers=new_connector.read_db_creds()['headers'], data=pin_payload)
            response = requests.request("POST", new_connector.read_db_creds()['invoke_url_user'], headers=new_connector.read_db_creds()['headers'], data=user_payload)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


