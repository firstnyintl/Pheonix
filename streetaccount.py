import numpy as np
import imaplib
import email
import dateutil.parser
import nltk
import BBG
import pdb


class SAMessage:
    """
    Contains information about a single message-- e.g. an upgrade, downgrade, initiation, etc. -- as well as several functions that can be computed using information from Bloomberg
    """
    def __init__(self, ticker, type, firm, analyst, PT, old_PT, date):
        self.ticker = ticker
        self.type = type
        self.firm = firm
        self.rating = rating
        self.analyst = analyst
        self.PT = float(PT)
        self.old_PT = float(old_PT)
        self.date = date
        self.BBGData = BBG.getBulkAnalystRecs(self.ticker)

    def stHighLow(self):
        """
        Returns string to indicate whether Price Target is street high or low
        """
        if self.PT > np.max(self.ptlist):
            return 'Street High'
        elif self.PT < np.min(self.ptlist):
            return 'Street Low'
        else:
            return 'Neither'

    def quartile(self):
        """
        Returns which quartile the new price target falls under
        """
        ptlist = self.BBGData[self.BBGData['Target Price'] > 0]['Target Price'].values
        high = np.max(ptlist)
        rng = np.ptp(ptlist)
        q = rng / 4.
        if self.PT > high - q:
            return 4
        elif self.PT > high - q * 2:
            return 3
        elif self.PT > high - q * 3:
            return 2
        else:
            return 1

    def pctChange(self):
        """
        Returns the % change between the new and old price targets
        """
        change = self.PT - self.old_PT
        pctDif = change / self.old_PT
        result = pctDif*100
        return result

    # def changeLongTime(self):
    #     """
    #     calculated how long its been since the anayalts last peice and if that length of time is longer than 1 year, True is returned
    #     """
    #     today = self.date
    #     numberofDays = (today - 'weird ass bloomberg date').days
    #     if numberofDays > 360:
    #         return True
    #     else:
    #         return False

    def newAnalyst(self):
        """
        Returns True if there is a new analyst
        """
        return not self.BBGData[self.BBGData['Firm Name'] == self.firm].Analyst.values[0] == self.analyst.upper()

    def shortInt(self):
        """
        Returns True if short interest is above a significant threshold
        """
        shrtInt = BBG.getSingleField(self.ticker, 'SHORT_INT_RATIO')
        return shrtInt > 5


def getMessages():

    def get_text_block(email_message_instance):
        maintype = email_message_instance.get_content_maintype()
        if maintype == 'multipart':
            for part in email_message_instance.get_payload():
                if part.get_content_maintype() == 'text':
                    return part.get_payload()
        elif maintype == 'text':
            return email_message_instance.get_payload()

    # Login to GMAIL
    mail = imaplib.IMAP4_SSL('imap.gmail.com')
    mail.login("ratingsstock", "firstny2015")
    mail.list()
    mail.select("inbox")

    # Get all messages and find the last message
    result, data = mail.uid('search', None, "ALL")
    latest_email_uid = data[0].split()[-1]
    result, data = mail.uid('fetch', latest_email_uid, "(RFC822)")
    raw_email = data[0][1]

    # Generate email message object from raw message
    email_message = email.message_from_string(raw_email)

    # Convert to text block
    block = get_text_block(email_message)

    # Get rid of header and footer
    block = block.split('StreetAccount')[1:3]
    block = "".join(block)

    # Get rid of =, /r, /n
    block = block.translate(None, '=\r\\')
    block = block.translate(None, '\n')

    # Tokenize using NLTK
    tokenized_block = nltk.word_tokenize(block)

    # Isolate message from email
    index = tokenized_block.index('ET')
    msg = tokenized_block[index-1:]

    # These words indicate multiple events per msg
    multiple_msg_tokens = ['upgrades', 'downgrades', 'initiates', 'resumes', 'reinstates', 'assumes']
    # Two-word ratings contain these words first
    two_word_rating_list = ['sector', 'market']

    # Check whether is single upgrade / downgrade / initiation / resumption
    if not any([e in msg for e in multiple_msg_tokens]):
        SINGLE_MESSAGE_TICKER_INDEX = 5
        ticker = msg[SINGLE_MESSAGE_TICKER_INDEX]
        if 'upgraded' in msg:
            msgtype = 'upgrade'
            ix = msg.index('upgraded')
            if msg[ix+2] in two_word_rating_list:
                rating = msg[ix+2] + ' ' + msg[ix+3]
            else:
                rating = msg[ix+2]
        elif 'downgraded' in msg:
            msgtype = 'downgrade'
            ix = msg.index('downgraded')
            if msg[ix+2] in two_word_rating_list:
                rating = msg[ix+2] + ' ' + msg[ix+3]
            else:
                rating = msg[ix+2]
        elif 'initiated' in msg:
            msgtype = 'initiates'
            ix = msg.index('initiated')
            if msg[ix+1] in two_word_rating_list:
                rating = msg[ix+1] + ' ' + msg[ix+2]
            else:
                rating = msg[ix+1]
        elif 'resumed' in msg:
            msgtype = 'resumption'
            ix = msg.index('resumed')
            if msg[ix+1] in two_word_rating_list:
                rating = msg[ix+1] + ' ' + msg[ix+2]
            else:
                rating = msg[ix+1]
        elif 'reinstated' in msg:
            msgtype = 'resumption'
            ix = msg.index('resumed')
            if msg[ix+1] in two_word_rating_list:
                rating = msg[ix+1] + ' ' + msg[ix+2]
            else:
                rating = msg[ix+1]
        elif 'assumed' in msg:
            msgtype = 'resumption'
            ix = msg.index('assumed')
            if msg[ix+1] in two_word_rating_list:
                rating = msg[ix+1] + ' ' + msg[ix+2]
            else:
                rating = msg[ix+1]
        else:
            msgtype = 'N/A'
    # Check whether upgrade, downgrade, or initiation, or resumption

    pdb.set_trace()
