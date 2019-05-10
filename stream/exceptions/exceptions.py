from src.configuration import email_alerts

class GeneralRankException(Exception):
	def __init__(self, exc_line_no):
		email_alerts.rank_update_exception_alert(exc_line_no)

class GeneralLevelException(Exception):
	def __init__(self, exc_line_no):
		email_alerts.level_update_exception_alert(exc_line_no)

class GeneralTimeException(Exception):
	def __init__(self, exc_line_no):
		email_alerts.time_update_exception_alert(exc_line_no)

class GeneralHistException(Exception):
	def __init__(self, vendor_cd, exc_line_no):
		email_alerts.hist_update_exception_alert(vendor_cd, exc_line_no)

class GeneralSyncException(Exception):
	def __init__(self, serv_name, exc_line_no):
		email_alerts.sync_exception_alert(serv_name, exc_line_no)

