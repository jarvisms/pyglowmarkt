
import requests
import json
import datetime

PT1M = "PT1M"
PT30M = "PT30M"
PT1H = "PT1H"
P1D = "P1D"
P1W = "P1W"
P1M = "P1M"
P1Y = "P1Y"

class Rate:
    pass

class Pence:
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"Pence({self.value})"
    def __str__(self):
        return ("%s p" % self.value) if isinstance(self.value, str) else ("%.2f p" % self.value)
    def unit(self):
        return "pence"

class KWH:
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"KWH({self.value})"
    def __str__(self):
        return ("%s kWh" % self.value) if isinstance(self.value, str) else ("%.3f kWh" % self.value)
    def unit(self):
        return "kWh"

class Unknown:
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"Unknown({self.value})"
    def __str__(self):
        return "%s" % self.value
    def unit(self):
        return "unknown"

class VirtualEntity:
    def __repr__(self):
        return f"<glowmarkt VirtualEntity {self.id} '{self.name}'>"
    def get_resources(self):
        return self.client.get_resources(self.id)

class Tariff:
    pass

class Resource:
    def __repr__(self):
        return f"<glowmarkt Resource {self.id} '{self.name}'>"
    def get_readings(self, t_from, t_to, period, func="sum", nulls=False):
        return self.client.get_readings(self.id, t_from, t_to, period, func, nulls)
    def get_current(self):
        return self.client.get_current(self.id)
    def last_time(self):
        return self.client.last_time(self.id)
    def first_time(self):
        return self.client.first_time(self.id)
    def get_meter_reading(self):
        return self.client.get_meter_reading(self.id)
    def get_tariff(self):
        return self.client.get_tariff(self.id)
    def round(self, when, period):
        return self.client.round(when, period)
    def catchup(self):
        return self.client.catchup(self.id)
    def largereadings(self, t_from, t_to, period, func="sum", nulls=False):
        return self.client.largereadings(self, t_from, t_to, period, func, nulls)

class BrightClient:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.application = "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"
        self.url = "https://api.glowmarkt.com/api/v0-1/"
        self.session = requests.Session()

        self.token = self.authenticate()

    def authenticate(self):

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application
        }

        data = {
            "username": self.username,
            "password": self.password
        }

        url = f"{self.url}auth"

        resp = self.session.post(url, headers=headers, data=json.dumps(data))

        if resp.status_code != 200:
            raise RuntimeError("Authentication failed")

        resp = resp.json()

        if resp["valid"] == False:
            raise RuntimeError("Expected an authentication token")

        if "token" not in resp:
            raise RuntimeError("Expected an authentication token")

        return resp["token"]

    def get_virtual_entities(self):

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application,
            "token": self.token
        }

        url = f"{self.url}virtualentity"

        resp = self.session.get(url, headers=headers)

        if resp.status_code != 200:
            raise RuntimeError("Request failed")

        resp = resp.json()

        ves = []

        for elt in resp:

            ve = VirtualEntity()

            ve.client = self

            ve.application = elt["applicationId"]
            ve.type_id = elt["veTypeId"]
            ve.id = elt["veId"]

            if "postalCode" in elt:
                ve.postal_code = elt["postalCode"]
            else:
                ve.postal_code = None

            if "name" in elt:
                ve.name = elt["name"]
            else:
                ve.name = None

            ves.append(ve)

        return ves

    def get_resources(self, ve):

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application,
            "token": self.token
        }

        url = f"{self.url}virtualentity/{ve}/resources"

        resp = self.session.get(url, headers=headers)

        if resp.status_code != 200:
            raise RuntimeError("Request failed")

        resp = resp.json()

        resources = []

        for elt in resp["resources"]:
            r = Resource()
            r.id = elt["resourceId"]
            r.type_id = elt["resourceTypeId"]
            r.name = elt["name"]
            r.classifier = elt["classifier"]
            r.description = elt["description"]
            r.base_unit = elt["baseUnit"]

            r.client = self

            resources.append(r)
            
        return resources

    def round(self, when, period):

        # Work out a rounding value.  Readings seem to be more accurate if
        # rounded to the near thing...
        if period == "PT1M":
            when = when.replace(second=0, microsecond=0)
        elif period == "PT30M":
            when = when.replace(minute=int(when.minute / 30),
                                second=0,
                                microsecond=0)
        elif period == "PT1H":
            when = when.replace(minute=0,
                                second=0,
                                microsecond=0)
        elif period == "P1D":
            when = when.replace(hour=0, minute=0,
                                second=0,
                                microsecond=0)
        elif period == "P1W":
            when = when.replace(hour=0, minute=0,
                                second=0,
                                microsecond=0)
        elif period == "P1M":
            when = when.replace(day=1, hour=0, minute=0,
                                second=0,
                                microsecond=0)
        else:
            raise RuntimeError("Period %s not known" % period)

        return when

    def get_readings(self, resource, t_from, t_to, period, func="sum", nulls=False):

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application,
            "token": self.token
        }

        utc = datetime.timezone.utc

        def time_string(x):
            """Converts a date or naive datetime instance to a timezone aware datetime instance in UTC and returns a time string in UTC. If its already timezone aware, it will still convert to UTC"""
            if isinstance(x, datetime.datetime):
                x = x.astimezone(utc)  # Converts local timezone with DST, or pre-specified timezones into UTC
                return x.replace(tzinfo=None).isoformat()   # Remove TZ data without conversion (it's alreadu in UTC), then return ISO string format
            elif isinstance(x, datetime.date):
                x = datetime.datetime.combine(x, datetime.time()).astimezone(utc)   # Adds midnight time components (assumed in local timezone) and converts to UTC
                return x.replace(tzinfo=None).isoformat()   # Would be similar to t_to.isoformat()[:19] where the +01:00 part is just truncated
            else:
                raise RuntimeError("to_from/t_to should be date/datetime")

        # Convert to UTC datetimes, conversion will handle tz/dst conversions
        t_from = time_string(t_from)
        t_to = time_string(t_to)

        params = {
            "from": t_from,
            "to": t_to,
            "period": period,
            "offset": 0,    # Enforce UTC server side, because we handle TZ/DST conversions client side
            "function": func,
            "nulls": 1 if nulls else 0,     # nulls=1 will give None instead of Zero for missing data so its easy to discriminate between missing data or a genuine zero usage
        }

        url = f"{self.url}resource/{resource}/readings"

        resp = self.session.get(url, headers=headers, params=params)

        if resp.status_code != 200:
            raise RuntimeError("Request failed")

        resp = resp.json()

        if resp["units"] == "pence":
            cls = Pence
        elif resp["units"] == "kWh":
            cls = KWH
        else:
            cls = Unknown

        return [
            [datetime.datetime.fromtimestamp(v[0], tz=utc).astimezone(),     # Timezone and DST aware datetime in local timezone converted from server side UTC
             cls(v[1])]
            for v in resp["data"]   # v[1] is None when nulls=1 and no data exists
        ]

    def get_current(self, resource):

        # Tried it against the API, no data is returned

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application,
            "token": self.token
        }

        utc = datetime.timezone.utc

        url = f"{self.url}resource/{resource}/current"

        resp = self.session.get(url, headers=headers)

        if resp.status_code != 200:
            print(resp.text)
            raise RuntimeError("Request failed")

        resp = resp.json()

        if len(resp["data"]) < 1:
            raise RuntimeError("Current reading not returned")

        if resp["units"] == "pence":
            cls = Pence
        elif resp["units"] == "kWh":
            cls = KWH
        else:
            cls = Unknown

        print(datetime.datetime.fromtimestamp(resp["data"][0][0]), tz=utc).astimezone()

        return [
            datetime.datetime.fromtimestamp(resp["data"][0][0], tz=utc).astimezone(),
            cls(resp["data"][0][1])
        ]

    def last_time(self, resource):

        # Get the timeof the more recent available reading

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application,
            "token": self.token
        }

        utc = datetime.timezone.utc

        url = f"{self.url}resource/{resource}/last-time"

        resp = self.session.get(url, headers=headers)

        if resp.status_code != 200:
            print(resp.text)
            raise RuntimeError("Request failed")

        resp = resp.json()

        return datetime.datetime.fromtimestamp(resp["data"]["lastTs"], tz=utc).astimezone()

    def first_time(self, resource):

        # Get the time of the first available reading

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application,
            "token": self.token
        }

        utc = datetime.timezone.utc

        url = f"{self.url}resource/{resource}/first-time"

        resp = self.session.get(url, headers=headers)

        if resp.status_code != 200:
            print(resp.text)
            raise RuntimeError("Request failed")

        resp = resp.json()

        return datetime.datetime.fromtimestamp(resp["data"]["firstTs"], tz=utc).astimezone()


    def catchup(self, resource):

        # Tried it against the API, no data is returned

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application,
            "token": self.token
        }

        url = f"{self.url}resource/{resource}/catchup"

        resp = self.session.get(url, headers=headers)

        if resp.status_code != 200:
            print(resp.text)
            raise RuntimeError("Request failed")

        resp = resp.json()

        return resp

    def get_meter_reading(self, resource):

        # Tried it against the API, an error is returned
        raise RuntimeError("Not implemented.")

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application,
            "token": self.token
        }

        utc = datetime.timezone.utc

        url = f"{self.url}resource/{resource}/meterread"

        resp = self.session.get(url, headers=headers)

        if resp.status_code != 200:
            raise RuntimeError("Request failed")

        resp = resp.json()

        if len(resp["data"]) < 1:
            raise RuntimeError("Meter reading not returned")

        if resp["units"] == "pence":
            cls = Pence
        elif resp["units"] == "kWh":
            cls = KWH
        else:
            cls = Unknown

        return [
            [datetime.datetime.fromtimestamp(v[0], tz=utc).astimezone(), cls(v[1])]
            for v in resp["data"]
        ]

    def get_tariff(self, resource):

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application,
            "token": self.token
        }

        url = f"{self.url}resource/{resource}/tariff"

        resp = self.session.get(url, headers=headers)

        if resp.status_code != 200:
            raise RuntimeError("Request failed")

        resp = resp.json()

        ts = []

        for elt in resp["data"]:

            t = Tariff()
            t.name = elt["name"]
            t.commodity = elt["commodity"]
            t.cid = elt["cid"]
            t.type = elt["type"]

            rt = Rate()
            rt.rate = Pence(elt["currentRates"]["rate"])
            rt.standing_charge = Pence(elt["currentRates"]["standingCharge"])
            rt.tier = None
            
            t.current_rates = rt

            # rts = []
            # for elt2 in elt["structure"]:

            #     rt = Rate()
            #     rt.rate = elt2["planDetail"]["rate"]
            #     rt.standing_charge = elt2["planDetail"]["standing"]
            #     rt.tier = elt2["planDetail"]["tier"]
            #     rts.append(rt)

            # t.structure = rts
        
        return t

    def largereadings(self, resource, t_from=None, t_to=None, period=PT30M, func="sum", nulls=False):
        """
        Returns a list of readings for the resource and timespan.
        Output should be the same as for 'get_readings' and arguments are also the same.

        A potentially very large query will be chunked into numerous smaller transactions
        using the 'get_readings' method based on the time limit for the period parameter
        and provides the result as if one single transaction was completed;
        a very large list of readings from all constituent underlying transactions.

        This method utilises the simpler 'get_readings' method and all other arguments are
        as per 'get_readings' and are passed through.

        This method can be used as a seamless replacement for 'get_readings'
        """
        timeLimits = {  # These limits are from the API spec
                PT1M    : datetime.timedelta(days=2, hours=23, minutes=59), # Found empirically, not in spec
                PT30M   : datetime.timedelta(days=10),
                PT1H    : datetime.timedelta(days=31),
                P1D     : datetime.timedelta(days=31),
                P1W     : datetime.timedelta(weeks=6),
                P1M     : datetime.timedelta(days=366),
                P1Y     : datetime.timedelta(days=366),
        }
        periodTimedelta = {
                PT1M    : datetime.timedelta(minutes=1),
                PT30M   : datetime.timedelta(minutes=30),
                PT1H    : datetime.timedelta(hours=1),
                P1D     : datetime.timedelta(days=1),
                P1W     : datetime.timedelta(days=7),
                P1M     : datetime.timedelta(days=31),  # Month lengths are accounted for later
                P1Y     : datetime.timedelta(days=366), # Accounts for leap years
        }
        period = period.upper()
        if period not in timeLimits:
            raise TypeError("The period is not supported")
        ptd = periodTimedelta[period]
        maxwindow = timeLimits[period]
        if maxwindow < ptd:
            raise TypeError("Max window is smaller than the period")
        if None in (t_from, t_to):
            raise TypeError("You must explicitly provide a t_from and t_to")
        if isinstance(t_from, datetime.datetime):
            t_from = t_from.astimezone(datetime.timezone.utc)
        elif isinstance(t_from, datetime.date):
            t_from = datetime.datetime.combine(t_from, datetime.time(), datetime.timezone.utc)
        else:
            raise TypeError("t_from isn't a 'date' or 'datetime' instance")
        if isinstance(t_to, datetime.datetime):
            t_to = t_to.astimezone(datetime.timezone.utc)
        elif isinstance(t_to, datetime.date):
            t_to = datetime.datetime.combine(t_to, datetime.time(), datetime.timezone.utc)
        else:
            raise TypeError("t_to isn't a 'date' or 'datetime' instance")
        if t_to < t_from: # Swap dates if reversed
            t_from, t_to = t_to, t_from
        elif t_to == t_from:
            raise TypeError("t_from and t_to are the same")
        if (
            (period == PT1M and not ( # Check its a clean minute
            t_to.microsecond == t_to.second == t_from.microsecond == t_from.second == 0
            )) or
            (period == PT30M and not ( # Check its a clean half hour
            t_to.microsecond == t_to.second == t_from.microsecond == t_from.second == 0 and
            t_from.minute in (0,30) and t_to.minute in (0,30)
            )) or
            (period == PT1H and not ( # Check its a clean hour
            t_to.microsecond == t_to.second == t_to.minute == t_from.microsecond == t_from.second == t_from.minute == 0
            )) or
            (period == P1D and not ( # Check its a clean day
            t_to.microsecond == t_to.second == t_to.minute == t_to.hour == t_from.microsecond == t_from.second == t_from.minute == t_from.hour == 0
            )) or
            (period == P1W and not ( # Check its a clean week starting on Monday and ending on Sunday
            t_to.microsecond == t_to.second == t_to.minute == t_to.hour == t_from.microsecond == t_from.second == t_from.minute == t_from.hour == 0 and
            (t_from.weekday(), t_to.weekday()) == (0,6)
            )) or
            (period == P1M and not ( # Check its a clean month starting on the 1st and ending on whatever the last day of the month is for that month and year
            t_to.microsecond == t_to.second == t_to.minute == t_to.hour == t_from.microsecond == t_from.second == t_from.minute == t_from.hour == 0 and
            t_from.day == 1 and t_to.day == ((t_to.replace(month=t_to.month+1, day=1) if t_to.month < 12 else t_to.replace(year=t_to.year+1, month=1, day=1)) - datetime.timedelta(days=1)).day
            )) or
            (period == P1Y and not ( # Check its a clean year starting on the 1st Jan and ending on 31st Dec
            t_to.microsecond == t_to.second == t_to.minute == t_to.hour == t_from.microsecond == t_from.second == t_from.minute == t_from.hour == 0 and
            t_from.day == 1 and t_from.month == 1 and t_to.day == 31 and t_to.month == 12
            ))
            ):
            raise TypeError("t_from and t_to must be aligned with the period")
        reqwindow = t_to - t_from # Requested window/duration
        print(f"{reqwindow} requested and the maximum limit is {maxwindow} for {period}")
        if reqwindow <= maxwindow: # If the period is smaller than max, use directly
            print("Only 1 transaction is needed")
            return resource.get_readings(t_from=t_from, t_to=t_to, period=period, func=func, nulls=nulls)
        else:   # If the period is larger than max, then break it down
            if period == P1M:
                reqperiods = (t_to.year-t_from.year)*12 + (t_to.month-t_from.month) + 1 # Requested duration counted in months
            else:
                reqperiods = reqwindow // ptd # Requested duration in periods
            maxperiods = maxwindow // ptd # Maximum duration in periods
            d = 2   # Start by dividing into 2 intervals, since 1 was tested above
            while True:
                # Divide into incrementally more/smaller peices and check
                i, r = divmod(reqperiods, d)
                # Once the peices are small enough, stop.
                # If there is no remainder, take i, otherwise the remainders will
                # be added to other peices so add 1 and check that instead.
                if (i+1 if r else i) <= maxperiods: break
                d += 1
            # Make a list of HH sample sizes, with the remainders added onto the
            # first sets. Such as 11, 11, 10 for a total of 32.
            periodsBlocks = [i+1]*r + [i]*(d-r)
            print(f"{len(periodsBlocks)} transactions will be used")
            Intervals=[]
            IntervalStart = t_from   # The first t_from is the original start
            if period == P1M:
                for i in periodsBlocks:
                    # Add calculated number of half hours on to the start time
                    IntervalEnd = IntervalStart + i * ptd
                    IntervalEnd = IntervalEnd.replace(day=1) - datetime.timedelta(days=1)
                    # Define each sample window and start the next one after the last
                    Intervals.append({"t_from":IntervalStart,"t_to":IntervalEnd})
                    IntervalStart = IntervalEnd + datetime.timedelta(days=1)
            else:
                for i in periodsBlocks:
                    # Add calculated number of half hours on to the start time
                    IntervalEnd = IntervalStart + i * ptd
                    # Define each sample window and start the next one after the last
                    Intervals.append({"t_from":IntervalStart,"t_to":IntervalEnd})
                    IntervalStart = IntervalEnd + ptd
        # Gather results
        result = []
        for chunk in Intervals:
            chunkresult = resource.get_readings(period=period, **chunk, func=func, nulls=nulls)
            result.extend(chunkresult)
        return result
