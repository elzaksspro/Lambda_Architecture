class Activity:
    def __init__(self,timestamp_hour,referrer,action,prevPage,visitor,page,product):
        self.timestamp_hour = timestamp_hour
        self.referrer = referrer
        self.action = action
        self.prevPage = prevPage
        self.visitor = visitor
        self.page = page
        self.product = product