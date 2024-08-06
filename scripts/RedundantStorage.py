from sqlalchemy import text
import pandas as pd
from ErrorHandler import ErrorHandler

class RedundantStorage(ErrorHandler):

    def __init__(self,log_type):
        super().__init__(log_type)

    def storeDataSch(self,engine):
