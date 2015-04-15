import datetime
import re
import numpy as np
import pandas as pd
import imaplib
import email
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
        """
        Process raw email string, returns decoded message string
        """
        maintype = email_message_instance.get_content_maintype()
        if maintype == 'multipart':
            for part in email_message_instance.get_payload():
                if part.get_content_maintype() == 'text':
                    return part.get_payload(decode=True)
        elif maintype == 'text':
            return email_message_instance.get_payload(decode=True)

    def getCurrency(PT, msg, PTix):
        """
        Process currecy identifiers for PT subsection of msg, returns currency string
        """
        # If PT field doesnt contain currency identifier
        if PT == msg[PTix]:
            curr = msg[PTix-1]
            # If identifier is $
            if curr == '$':
                # If 'C' precedes $, currency is CAD
                if msg[PTix-2] == 'C':
                    return 'CAD'
                # If 'C' precedes $, currency is CAD
                if msg[PTix-2] == 'A':
                    return 'AUD'
                # If no 'C' precedes $, currency is USD
                else:
                    return 'USD'
            else: return curr
        else:
            curr = ''.join(i for i in msg[PTix] if not i.isdigit())
            # Check if 'p' in identifier, return GBP
            if 'p' in curr:
                return 'GBP'
            # Check if Euro sign
            if curr == '\x80':
                return 'EUR'
            return curr

    def getPT(msg):
        if "Target" in msg: ix = msg.index('Target')
        elif 'target' in msg: ix = msg.index('target')
        else: return '', ''
        for i, x in enumerate(msg[ix:]):
            # Check if contains float
            if len(re.findall('\d+.\d+', x)) > 0:
                PT = re.findall('\d+.\d+', x)[0]
                curr = getCurrency(PT, msg, ix+i)
                PT = float(PT.replace(',', ''))
                break
            # Check if contains int
            elif len(re.findall('\d+', x)) > 0:
                PT = re.findall('\d+', x)[0]
                curr = getCurrency(PT, msg, ix+i)
                PT = float(PT.replace(',', ''))
                break
            # If doesn't contain float or int continue
            else:
                continue
        return PT, curr

    def getSingle(msg):
        """
        Check if single message and process message

        Returns message parameters, as well as True/False whether single or not
        """

        # Check whether is single upgrade / downgrade / initiation / resumption
        if not any([e in msg for e in multiple_msg_tokens]):

            # Get ticker
            SINGLE_MESSAGE_TICKER_INDEX = 5
            ticker = msg[SINGLE_MESSAGE_TICKER_INDEX]

            # Get Analyst (assumes format "Analyst is xx xx")
            if "Analyst" in msg:
                ix = msg.index('Analyst')
                analyst = msg[ix+2] + ' ' + msg[ix+3]
            else:
                analyst = ''

            # Get message type and rating

            # Check if target price increase or decrease
            # Assumed format 'target in/decreased to x' max 5 spaces after ticker
            if 'target' in msg[SINGLE_MESSAGE_TICKER_INDEX:SINGLE_MESSAGE_TICKER_INDEX+5]:
                ix = msg.index('target')
                if msg[ix+1] == 'increased':
                    msgtype = 'PT increased'
                    PT, curr = getPT(msg)
                    rating = ''
                elif msg[ix+1] == 'decreased':
                    msgtype = 'PT decreased'
                    PT, curr = getPT(msg)
                    rating = ''
                else:
                    raise Exception('Message type not accounted for')
                return True, [ticker, msgtype, rating, PT, curr, analyst]
            # Assumed format "upgraded to x"
            else:
                if 'upgraded' in msg:
                    msgtype = 'upgrade'
                    ix = msg.index('upgraded')
                    if msg[ix+2] in two_word_rating_list:
                        rating = msg[ix+2] + ' ' + msg[ix+3]
                    else:
                        rating = msg[ix+2]
                # Assumed format "downgraded to x"
                elif 'downgraded' in msg:
                    msgtype = 'downgrade'
                    ix = msg.index('downgraded')
                    if msg[ix+2] in two_word_rating_list:
                        rating = msg[ix+2] + ' ' + msg[ix+3]
                    else:
                        rating = msg[ix+2]
                # Assumed format "initiated x"
                elif 'initiated' in msg:
                    msgtype = 'initiation'
                    ix = msg.index('initiated')
                    if msg[ix+1] in two_word_rating_list:
                        rating = msg[ix+1] + ' ' + msg[ix+2]
                    else:
                        rating = msg[ix+1]
                # Assumed format "resumed x"
                elif 'resumed' in msg:
                    msgtype = 'resumption'
                    ix = msg.index('resumed')
                    if msg[ix+1] in two_word_rating_list:
                        rating = msg[ix+1] + ' ' + msg[ix+2]
                    else:
                        rating = msg[ix+1]
                # Assumed format "reinstated x"
                elif 'reinstated' in msg:
                    msgtype = 'resumption'
                    ix = msg.index('reinstated')
                    if msg[ix+1] in two_word_rating_list:
                        rating = msg[ix+1] + ' ' + msg[ix+2]
                    else:
                        rating = msg[ix+1]
                # Assumed format "reinstated x"
                elif 'reinitiated' in msg:
                    msgtype = 'resumption'
                    ix = msg.index('reinitiated')
                    if msg[ix+1] in two_word_rating_list:
                        rating = msg[ix+1] + ' ' + msg[ix+2]
                    else:
                        rating = msg[ix+1]
                # Assumed format "assumed x"
                elif 'assumed' in msg:
                    msgtype = 'resumption'
                    ix = msg.index('assumed')
                    if msg[ix+1] in two_word_rating_list:
                        rating = msg[ix+1] + ' ' + msg[ix+2]
                    else:
                        rating = msg[ix+1]
                else:
                    return True, None

                # Get Price Target and currency
                PT, curr = getPT(msg)
                return True, [ticker, msgtype, rating, PT, curr, analyst]
        return False, None

    # Login to GMAIL
    mail = imaplib.IMAP4_SSL('imap.gmail.com')
    mail.login("ratingsstock", "firstny2015")
    mail.list()
    mail.select("inbox")

    # Get all unread messages
    x, y = mail.uid('search', None, "(UNSEEN)")

    # Check if unread emails
    unreadMails = len(y[0].split()) > 0

    # While there are unread emails
    while(unreadMails):

        # Get number of unread emails
        latest_email_uid = y[0].split()[-1]
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

        # Get Date
        dt = msg[2] + ' ' + msg[0]
        date = datetime.datetime.strptime(dt, '%m/%d/%y %H:%M')

        # These words indicate multiple events per msg
        multiple_msg_tokens = ['upgrades', 'downgrades', 'initiates', 'resumes', 'reinstates', 'assumes', 'notable']
        # Two-word ratings contain these words first
        two_word_rating_list = ['sector', 'market']

        # Get firm name
        firm_list = pd.DataFrame.from_csv('research_firm_list.csv')
        firm_list['eval'] = firm_list['Unique Identifier'].isin(msg)
        try:
            firm = firm_list[firm_list['eval']].index.values[0]
        except:
            firm = ''

        # Check if email only contains one message (one ticker)
        isSingle, vals = getSingle(msg)
        if isSingle:
            if vals is None:
                print '------------------------------'
                print '        NEW MESSAGE           '
                print '------------------------------'
                print ' Can\'t process message type  '
                print ''
            else:
                ticker = vals[0]
                msgtype = vals[1]
                rating = vals[2]
                PT = vals[3]
                curr = vals[4]
                analyst = vals[5]

                print '------------------------------'
                print '        NEW MESSAGE           '
                print '------------------------------'
                print 'Date: ' + str(date)
                print 'Firm: ' + firm
                print 'Ticker: ' + ticker
                print 'Type: ' + msgtype
                print 'Rating: ' + rating
                print 'PT: ' + str(PT)
                print 'FX: ' + curr
                print 'Analyst: ' + analyst
                print ''
        else:
            print '------------------------------'
            print '        NEW MESSAGE           '
            print '------------------------------'
            print 'Can\'t process more than one msg'
            print ''


        # Again get all unread messages
        x, y = mail.uid('search', None, "(UNSEEN)")

        # Check if unread emails
        unreadMails = len(y[0].split()) > 0


getMessages()
