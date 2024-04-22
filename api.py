
connect = msal.ConfidentialClientApplication(
    config["client_id"], authority=config["authority"],
    client_credential=config["secret"]
    )
result = connect.acquire_token_silent(config["scope"], account=None)

if not result:
    result = connect.acquire_token_for_client(scopes=config["scope"])  
data=system.tag.readBlocking('[default]Tank Farm/Global/Tank_List')[0].value
for tankNum in system.dataset.toPyDataSet(data):
	if 'PT' not in tankNum[0]:
		tank='T'+str(tankNum[0]).zfill(3)
		if "access_token" in result:
	        # print('Token Aquired')
			token='Bearer {}'.format(result['access_token'])    
			# please note- use dataareaId filter and cross-company to get better results 
			# queryParams = "/data/InventoryMovementJournalEntriesV2?$filter=Warehouse eq 'ANG-TF' and dataAreaId eq '5150' &cross-company=true"
			queryParams ="/data/InventSums?"
			queryParams+="$filter=dataAreaId eq '5150' and InventLocationId eq 'ANG-TF' and AvailPhysical ne 0"
			queryParams+="and wMSLocationId eq '"+tank+"' &cross-company=true&$orderby=SPR_ModifiedDateTime desc&$top=1"
			productionOrdersUrl = '{}{}'.format(config["ActiveDirectoryResource"],queryParams)
			data = requests.get(  # Use token to call downstream service
			    url=productionOrdersUrl,headers={'Authorization': token}).json()
			# print(data)
			tankData={'ProductName':None,'SPR_ModifiedDateTime':None,'AvailPhysical':None,'ItemId':None,'InventColorId':None,'Product':None}
			for row in (data['value']):
			    tankData['ProductName']=row['ProductName']
			    tankData['SPR_ModifiedDateTime']=row['SPR_ModifiedDateTime'].replace('T',' ').replace('Z',' ')
			    tankData['AvailPhysical']=row['AvailPhysical']
			    tankData['ItemId']=row['ItemId']
			    tankData['InventColorId']=row['InventColorId']
			    tankData['Product']=row['Product']
			print(tank,tankNum[0],tankData['ProductName'],tankData['InventColorId'],tankData['ItemId'],tankData['SPR_ModifiedDateTime'],tankData['AvailPhysical'],tankData['Product'])
			try:
				q="INSERT INTO `Tanks_API` (`TankId`, `Tank_Number`, `Description`, `Customer`, `ItemId`, `d365_Time`,`litres`,`Product`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
				system.db.runPrepUpdate(q, [tank,tankNum[0],tankData['ProductName'],tankData['InventColorId'],tankData['ItemId'],tankData['SPR_ModifiedDateTime'],tankData['AvailPhysical'],tankData['Product']], 'nas_development')
			except:
				pass	
