class DBConfig:
    @staticmethod
    def get_connection_url():
        return 'mysql+pymysql://root:root123456@localhost:3306/stock' 