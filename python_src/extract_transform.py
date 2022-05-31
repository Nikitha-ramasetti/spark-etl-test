import requests
import pandas as pd


def request_url(url):
    try:
        # extracting data via response and read JSON file
        res_url = requests.get(url)
        res_req = res_url.json()
    except Exception as err:
        return "Error"
    return res_req


# Extract and transform data
def users(endpoint1):
    # normalizing the JSON file and creating dataframe to obtain profile objects & column
    df_users = pd.json_normalize(endpoint1, sep='_')

    # handling the PII information by taking birth_year
    df_users["birthDate"] = pd.to_datetime(df_users["birthDate"])
    df_users["birth_year"] = (df_users["birthDate"].dt.year)

    # extract domain from emails field and removing all PIIs
    df_users["domain"] = df_users["email"].str.split("@").str[1]
    df_users = df_users.drop(
        columns=["firstName", "lastName", "address", "zipCode", "subscription", "birthDate", "email"]
    )

    # rename the column id to user_id
    df_users["createdat"] = pd.to_datetime(df_users["createdAt"])
    df_users["updatedat"] = pd.to_datetime(df_users["updatedAt"])
    df_users = df_users.rename(columns={"id": "user_id",
                                        "profile_isSmoking": "profile_is_smoking"})
    df_users = df_users[['user_id', 'createdat', 'updatedat', 'city', 'country', 'profile_gender', 'profile_is_smoking',
                         'profile_profession', 'profile_income', 'birth_year', 'domain']]

    return df_users


def subscriptions(endpoint1):
    # normalizing the JSON file and creating dataframe to extract subscription objects
    df_subsq = pd.json_normalize(endpoint1, meta=["id"], record_path=["subscription"], sep='_')

    # rename the column id to user_id
    df_subsq = df_subsq.rename(columns={"id": "user_id"})

    # hashing
    df_subsq["createdat"] = pd.to_datetime(df_subsq["createdAt"])
    df_subsq["startdate"] = pd.to_datetime(df_subsq["startDate"])
    df_subsq["enddate"] = pd.to_datetime(df_subsq["endDate"])

    df_subsq = df_subsq[['user_id', 'createdat', 'startdate', 'enddate', 'status', 'amount']]
    df_subsq["subscription_id"] = df_subsq.index + 1

    return df_subsq


def messages(endpoint2):
    # normalizing the JSON file and create dataframe
    df_msg = pd.json_normalize(endpoint2, sep='_')

    # rename the column id to message_id
    df_msg = df_msg.rename(columns={"id": "message_id",
                                    "receiverId": "receiverid",
                                    "senderId": "senderid"})

    df_msg["createdat"] = pd.to_datetime(df_msg["createdAt"])
    df_msg = df_msg.drop(columns=["message", "createdAt"])

    return df_msg