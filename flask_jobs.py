
class IJob:
    pass


class FacebookScrapeJob:
    def __init__(self):
        # TODO: load from config/db
        self.group_ids = []
        # login / vpn / etc

        pass

    def __call__(self, *args, **kwargs):
        pass
