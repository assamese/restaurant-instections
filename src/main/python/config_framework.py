
class ConfigFramework:
    # Postgres username, password, and database name
    POSTGRES_ADDRESS = 'raja.db.elephantsql.com'  ## INSERT YOUR DB ADDRESS
    POSTGRES_PORT = '5432'
    POSTGRES_USERNAME = 'ljalsmbf'
    POSTGRES_PASSWORD = ''  ## PLUG IN YOUR POSTGRES PASSWORD
    POSTGRES_DBNAME = 'ljalsmbf'  ## CHANGE THIS TO YOUR DATABASE NAME
    AWS_SECRET = ''  ## PLUG IN YOUR AWS SECRET

    @staticmethod
    def getPostgresStr():
        # A long string that contains the necessary Postgres login information
        postgres_str = (
            'postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=ConfigFramework.POSTGRES_USERNAME,
                                                                                    password=ConfigFramework.POSTGRES_PASSWORD,
                                                                                    ipaddress=ConfigFramework.POSTGRES_ADDRESS,
                                                                                    port=ConfigFramework.POSTGRES_PORT,
                                                                                    dbname=ConfigFramework.POSTGRES_DBNAME))
        return postgres_str

    @staticmethod
    def getPsycopg2Str():
        conn_str = "dbname=" \
        + ConfigFramework.POSTGRES_DBNAME \
        + " user=" \
        + ConfigFramework.POSTGRES_USERNAME \
        + " host=" \
        + ConfigFramework.POSTGRES_ADDRESS \
        + " password=" \
        + ConfigFramework.POSTGRES_PASSWORD

        return conn_str

    @staticmethod
    def getAWS_Secret():
        return ConfigFramework.AWS_SECRET
