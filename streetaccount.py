import datetime
import re
import numpy as np
from pandas import HDFStore, DataFrame, read_hdf
import imaplib
import email
import nltk
import pdb

# Database file
DB = 'DB.h5'


def processEvent(event):
    """
    Takes incoming event, calculates additional fields, writes Event to Database, updates ratings
    """
    # Load database file
    store = HDFStore(DB)

    # Database events path
    events_path = 'ratings/SAEvents'

    # Read event table
    df = store[events_path]

    # Convert Event to Strings
    event = [str(i) for i in event]

    # Generate index (increment largest existing index)
    index = df.index.max() + 1

    # Create DataFrame of event
    msg = DataFrame([event], columns=df.columns, index=[index])
    # msg = DataFrame([event], columns=['Date', 'Firm', 'Ticker', 'Type', 'Rating', 'PT', 'FX', 'Analyst'], index=[100000000])

    # Add event to event table
    store.append(events_path, msg, min_itemsize=50, format='table', data_columns=True)

    # End database access
    store.close()

    print '------------------------------'
    print '        NEW MESSAGE           '
    print '------------------------------'
    print 'Date: ' + event[0]
    print 'Firm: ' + event[1]
    print 'Ticker: ' + event[2]
    print 'Type: ' + event[3]
    print 'Rating: ' + event[4]
    print 'PT: ' + event[5]
    print 'FX: ' + event[6]
    print 'Analyst: ' + event[7]
    print ''

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

    def shortInt(self):
        """
        Returns True if short interest is above a significant threshold
        """
        shrtInt = BBG.getSingleField(self.ticker, 'SHORT_INT_RATIO')
        return shrtInt > 5


def addFirm(firmname, identifier):
    """
    Add firm name and unique identifier to DataBase
    """

    # Load database file
    store = HDFStore(DB)

    # Database path
    path = 'ratings/firms'

    # Read event table
    df = store[path]

    # Generate index (increment largest existing index)
    index = df.index.max() + 1

    # Create DataFrame of event
    msg = DataFrame([[firmname, identifier]], columns=df.columns, index=[index])

    # Add event to event table
    store.append(path, msg, min_itemsize=50, format='table', data_columns=True)

    # End database access
    store.close()


def getMessages():
    """
    Log into email, process unread emails, process events from emails, write events to database
    """

    def emailLogin():
        """
        Log into gmail using IMAP, return imaplib object
        """

        # Login information
        USER = 'ratingsstock'
        PW = 'firstny2015'
        SERVER = 'imap.gmail.com'
        MAIN_FOLDER = 'inbox'

        # Login and return mail object
        mail = imaplib.IMAP4_SSL(SERVER)
        mail.login(USER, PW)
        mail.list()
        mail.select(MAIN_FOLDER)
        return mail

    def getLastMsgCore(y, mail):
        """
        Pulls the first unread email and returns a processed core tokenized message
        """
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

        # Special case--wierd norweigan char
        block = block.replace('\xf8', 'o')

        # Tokenize using NLTK
        tokenized_block = nltk.word_tokenize(block)

        # Isolate message from email and return
        index = tokenized_block.index('ET')
        msg = tokenized_block[index-1:]
        return msg

    def getDate(msg):
        """
        Process date from message
        """
        # Sometimes (e.g. in case of revision), chars stuck in date, remove
        sub = re.sub('[^a-zA-Z_]+', '', msg[0])

        # Get Date
        if not sub: dt = msg[2] + ' ' + msg[0]
        else: dt = msg[2] + ' ' + msg[0].replace(sub, '')

        # Special case-- remove '.'
        dt = dt.replace('.', '')

        # Store date in datetime object
        date = datetime.datetime.strptime(dt, '%m/%d/%y %H:%M')
        return date

    def getFirm(msg):
        """
        Cross reference with firm database and return firm name, if nothing found add firm to database
        """
        # Database path to fim list
        path = 'ratings/firms'

        # Get firm list
        firms = read_hdf(DB, path)

        # Search whether message contains any firm identifier
        firms['eval'] = firms['Identifier'].isin(msg)

        # If identifier found in database, return firm name
        try:
            return firms[firms['eval']].Firm.values[0]

        # If identifier not found, return empty string
        except:
            return ''

    def getAnalyst(msg):
        """
        Extract analyst from message and return (assumed that message ends right after analyst or has another bullet)
        If nothing found, return empty string
        """
        # If "Analyst" is found in message
        if "Analyst" in msg:

            # Get index of occurence
            ix = msg.index('Analyst')

            # Get index of beginning of analyst name (is there "is" in between?)
            if msg[ix+1] != 'is': startix = ix+1
            else: startix = ix+2

            # Flatten rest of message
            msg_substring = ' '.join(msg[startix:])

            # If another bullet, set analyst name equal to rest of text before bullet
            if '*' in msg_substring: return msg_substring[:msg_substring.index('*')]

            # If no more bullets, assume end of message and set analyst to rest of message
            else: return msg_substring

        # If "Analyst" not found, return empty string
        else:
            return ''

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
        # If currency is not directly attached to PT field (e.g. ['SEK','25.7'] vs. ['SEK25.7']
        if PT == msg[PTix]:

            # Set currency equal to field immediately prior to PT field
            curr = msg[PTix-1]

            # If prior field is '$'
            if curr == '$':

                # If 'C' precedes $, return 'CAD'
                if msg[PTix-2] == 'C': return 'CAD'

                # If 'A' precedes $, return 'AUD'
                if msg[PTix-2] == 'A': return 'AUD'

                # If nothing precedes $, return 'USD'
                else: return 'USD'

            # If prior field is not '$', return string (in these cases the field is the currency string)
            else: return curr

        # If currency is directly attached to PT field (e.g. 'SEK25.7')
        else:

            # Seperate non digits from PT field
            curr = ''.join(i for i in msg[PTix] if not i.isdigit())

            # Get rid of potential "." left in string (e.g. '.SEK')
            curr = curr.replace('.', '')

            # If 'p' is identifier, return 'GBP' (e.g. '257p')
            if 'p' in curr: return 'GBP'

            # If '\x80' (Euro symbol) is identifier, return 'EUR' (e.g. '\x80270')
            if '\x80' in curr: return 'EUR'

            # If not 'GBP' or 'EUR', return extracted string (e.g. 'NOK', 'SEK', 'TWD')
            return curr

    def getPT(msg, typ='single'):
        """
        Get price target from message and return
        Behaves differently given whether single or multiple messages
        ---If 'single' will look for target after occurrence of 'Target'
        ---If 'multiple' will look for target after occurrence of ')'
        """
        # Look for occurrence of 'target' if single message
        if typ == 'single':
            if "Target" in msg: ix = msg.index('Target')
            elif 'target' in msg: ix = msg.index('target')
            else: return '',''

        # Look for occurrence of ')' if not single message
        else:
            if ')' in msg: ix = msg.index(')')
            else: return '',''

        # Loop through message after occurrence of "target"/"Target" until PT found
        for i, x in enumerate(msg[ix:]):

            # If a float is found
            if len(re.findall('\d+.\d+', x)) > 0:

                # Strip float (PT) from field
                PT = re.findall('\d+.\d+', x)[0]

                # Get currency
                curr = getCurrency(PT, msg, ix+i)

                # Convert string PT to float
                PT = float(PT.replace(',', ''))

                # Stop looking and return
                return PT, curr

            # If an int is found
            elif len(re.findall('\d+', x)) > 0:

                # Strip int (PT) from field
                PT = re.findall('\d+', x)[0]

                # Get currency
                curr = getCurrency(PT, msg, ix+i)

                # Convert string PT to float
                PT = float(PT.replace(',', ''))

                # Stop looking and return
                return PT, curr

        # If nothing found (misc. format), return empty strings
        return '', ''

    def getMsgType(msg):
        """
        Returns three values:
        --- boolean: whether message type was recognized
        --- string: message type
        --- string: rating
        """
        # If rating starts with one of the following, it is a two-word rating (e.g. "sector perform")
        two_word_rating_list = ['sector', 'market', 'strong']

        # Key/value dictionary for other message types in headline
        key_dict = {'upgraded': 'upgrade',
                    'downgraded': 'downgrade',
                    'initiated': 'initiation',
                    'resumed': 'resumption',
                    'reinstated': 'resumption',
                    're-instated': 'resumption',
                    'reinitiated': 'resumption',
                    're-initiated': 'resumption',
                    'assumed': 'resumption',
                    }

        # Message type recognized instantiated to False
        msgtype_recognized = False

        # Search dictionary for keywords
        for key, value in key_dict.iteritems():

            # If keyword found
            if key in msg:

                # Message recognized
                msgtype_recognized = True

                # Set type according to dictionary
                msgtype = value

                # Get index of keyword occurrence
                ix = msg.index(key)

                # If not followed by something like 'to', decrement index
                if len(msg[ix+1]) != 2: ix -= 1

                # Check if two-word rating
                if msg[ix+2] in two_word_rating_list:

                    # Return two-word rating
                    rating = msg[ix+2] + ' ' + msg[ix+3]

                # If keyword not in two_word_rating list
                else:

                    # Return single-word rating
                    rating = msg[ix+2]

        if msgtype_recognized:
            return msgtype_recognized, msgtype, rating
        else: return msgtype_recognized, None, None


    def getSingle(msg):
        """
        Check if single message and, if so, process message and return event values

        Returns a tuple: (boolean, list), where:
        --boolean: "True" if single event, otherwise "False"
        --list: [ticker, msgtype, rating, PT, curr, analyst]

        Will return "True, None" if message type not recognized
        Will return "False, None" if more than one event in message
        """

        # If msg contains any of the following, there are multiple events per message
        multiple_msg_tokens = ['upgrades', 'downgrades', 'initiates', 'resumes', 'reinstates', 'assumes', 'notable']

        # If no value in multiple_msg_tokens found in msg, assume single event
        if not any([e in msg for e in multiple_msg_tokens]):

            # Get ticker by looking at standard index in message
            SINGLE_MESSAGE_TICKER_INDEX = 5
            ticker = msg[SINGLE_MESSAGE_TICKER_INDEX]

            # Check if target price increase or decrease
            # Assumed format 'target in/decreased to x' before first bullet or +5 spaces after ticker (headline)
            try:
                stpindx = msg.index('*')
            except:
                stpindx = 5

            # If 'target' found in msg headline
            if 'target' in msg[SINGLE_MESSAGE_TICKER_INDEX:stpindx]:

                # Get index of occurence
                ix = msg.index('target')

                # If followed by 'increased' or 'raises'
                if msg[ix+1] == 'increased' or msg[ix-1] == 'raises':

                    # Set type to 'PT increased'
                    msgtype = 'PT increased'

                    # Get PT and currency
                    PT, curr = getPT(msg)

                    # Set rating to empty string
                    rating = ''

                # If followed by 'decreased' or 'lowers'
                elif msg[ix+1] == 'decreased' or msg[ix-1] == 'lowers':

                    # Set type to 'PT decreased'
                    msgtype = 'PT decreased'

                    # Get PT and currency
                    PT, curr = getPT(msg)

                    # Set rating to empty string
                    rating = ''

                # If not followed by one of the above, msg type not understood
                else:
                    return True, None

                # Return True for single event, return values
                return True, [ticker, msgtype, rating, PT, curr]

            # If not PT change, check for other msg types 
            else:

                # Get msg type and rating
                msgtype_recognized, msgtype, rating = getMsgType(msg)

                # If msg type not recognized, exit
                if not msgtype_recognized: return True, None

                # Get Price Target and currency
                PT, curr = getPT(msg)

                # Return True and values
                return True, [ticker, msgtype, rating, PT, curr]

        # More than one event in msg
        return False, None

    def getMultiple(msg, date, firm, analyst):
        """
        Process core with multiple events, will return generator of lists:
        List looks like this [ticker, msgtype, rating, PT, curr, analyst]
        """
        # Get index of first bullet, otherwise quit
        try:
            ix = msg.index('*')
            anotherBullet = True
        except:
            anotherBullet = False

        # Loop through multiple messages
        while anotherBullet:

            # Get index of next bullet if possible
            try:
                nextix = msg[ix+1:].index('*')

                # If next bullet immediately follows, this is the real bullet
                # E.g. Don't want the "Upgrade" bullet, want actual msg bullet
                if (nextix < 5):
                    ix += (nextix + 1)

                # Again get index of next bullet
                try:
                    nextix = msg[ix+1:].index('*')

                    # Current msg is until next bullet
                    currentMsg = msg[(ix+1):(ix+nextix+1)]

                # If no more bullets, current msg is until end of msg
                except:
                    currentMsg = msg[ix+1:]
                    anotherBullet = False

            # If no more bullets, current msg is until end of msg
            except:
                currentMsg = msg[ix+1:]
                anotherBullet = False

            # If bullet is followed immediately by "Analyst", continue (i.e. exit)
            if (msg[ix+1] == 'Analyst'): continue

            # Get index of '('
            indx = currentMsg.index('(')

            # Ticker follows '('
            ticker = currentMsg[indx+1]

            # Get msg type and rating
            msgtype_recognized, msgtype, rating = getMsgType(currentMsg)

            # Get PT and curr
            PT, curr = getPT(currentMsg, typ='multiple')

            # Increment bullet index to next bullet
            ix += (nextix + 1)

            # If msgtype not recognized print out 
            if not msgtype_recognized:
                print '------------------------------'
                print '        NEW MESSAGE           '
                print '------------------------------'
                print ' Can\'t process message type  '
                print ''
                continue

            yield [date, firm, ticker, msgtype, rating, PT, curr, analyst]

    def getNotable(msg, date):
        """
        Get messages for email with "Other notable research", will return generator of events
        List looks like this [ticker, msgtype, rating, PT, curr, analyst]
        """
        # Look for first bullet (before first '(')
        ix = msg.index('(')

        # Loop back from '(' until first bullet found, get index
        while msg[ix] != '*': ix -= 1

        anotherBullet = True

        while anotherBullet:

            # Get index of next bullet if possible
            try:
                nextix = msg[ix+1:].index('*')

                # Current msg is until next bullet
                currentMsg = msg[(ix+1):(ix+nextix+1)]

            except:
                anotherBullet = False

                # Current msg is until end of msg
                currentMsg = msg[ix+1:]

            # Check if current message contains PT, if so continue to next bullet
            if ('target' in currentMsg) or ('Target' in currentMsg): continue

            # Check if current message contains PT, if so continue to next bullet
            if ('analyst' in currentMsg) or ('Analyst' in currentMsg): continue

            # Get index of '('
            indx = currentMsg.index('(')

            # Ticker follows '('
            ticker = currentMsg[indx+1]

            # Get message type and rating
            msgtype_recognized, msgtype, rating = getMsgType(currentMsg)

            # Assume no PT / currency / analyst
            PT = ''
            curr = ''
            analyst = ''

            # Get firm
            firm = getFirm(currentMsg)

            # Check if followed by price target
            if anotherBullet:
                try:
                    tgtix = msg[ix+nextix+2:].index('*')

                    # Next msg is until next bullet
                    nextMsg = msg[(ix+nextix+2):(ix+nextix+tgtix+2)]

                except:
                    # Next msg is until end of msg
                    nextMsg = msg[(ix+nextix+2):]

                # If next message contains target
                if ('Target' in nextMsg) or ('target' in nextMsg):

                    # Get PT and curr
                    PT, curr = getPT(nextMsg)

                # If next message contains analyst
                if ('Analyst' in nextMsg) or ('analyst' in nextMsg):

                    # Get analyst name
                    analyst = getAnalyst(nextMsg)

            # Increment bullet index to next bullet
            ix += (nextix + 1)

            # If msgtype not recognized print out 
            if not msgtype_recognized:
                print '------------------------------'
                print '        NEW MESSAGE           '
                print '------------------------------'
                print ' Can\'t process message type  '
                print ''
                continue

            yield [date, firm, ticker, msgtype, rating, PT, curr, analyst]


    def processMsg(msg):
        """
        Process message, if successful, write to database, if not print why
        """
        # Look for date
        date = getDate(msg)

        # Check to see if "Other notable research calls"
        if msg[6] == 'notable':
            events = getNotable(msg, date)
            for e in events: processEvent(e)

        # If not "Other notable research calls"
        else:
            # Look for firm name
            firm = getFirm(msg)

            # Look for analyst name
            analyst = getAnalyst(msg)

            # Check if email only contains one event (one ticker)
            isSingle, vals = getSingle(msg)

            # If contains only one event
            if isSingle:

                # If doesn't recognize type print following
                if vals is None:
                    print '------------------------------'
                    print '        NEW MESSAGE           '
                    print '------------------------------'
                    print ' Can\'t process message type  '
                    print ''

                # If type recognized, write event to database
                else:
                    event = [date, firm, vals[0], vals[1], vals[2], vals[3], vals[4], analyst]
                    processEvent(event)

            # If more than one event, print following
            else:
                events = getMultiple(msg, date, firm, analyst)
                for e in events: processEvent(e)

    # Login to gmail and get mail object
    mail = emailLogin()

    # Get unread message ids
    x, y = mail.uid('search', None, "(UNSEEN)")

    # Check if there are unread emails
    unreadMails = len(y[0].split()) > 0

    # While there are unread emails
    while(unreadMails):

        # Get first unread email and get tokenized message
        msg = getLastMsgCore(y, mail)

        # Process message
        processMsg(msg)

        # Update unread email boolean
        x, y = mail.uid('search', None, "(UNSEEN)")
        unreadMails = len(y[0].split()) > 0

# If file is executed as script
if __name__ == '__main__':
    getMessages()
