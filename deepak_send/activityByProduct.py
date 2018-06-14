class activityByProduct:
    def __init__(self,product,timestamp_hour,purchase_count,add_to_cart_count,page_view_count):
        self.product = product
        self.timestamp_hour = timestamp_hour
        self.purchase_count = purchase_count
        self.add_to_cart_count = add_to_cart_count
        self.page_view_count = page_view_count