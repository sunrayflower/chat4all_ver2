# connector_instagram.py
import connector_base

CHANNEL = "Instagram"
GROUP_ID = "connector_instagram_group"

if __name__ == '__main__':
    db = connector_base.setup_mongodb()
    if db is not None:
        connector_base.run_connector(CHANNEL, GROUP_ID, db)