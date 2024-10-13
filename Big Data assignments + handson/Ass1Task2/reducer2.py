#!/usr/bin/env python3
import sys

currentTime = None
endpointStatus = {}
clientsProcessed = {}

for inputLine in sys.stdin:
    fields = inputLine.strip().split()

    if len(fields) == 7:
        reqId = fields[1]
        cliId = fields[2]
        apiEndpoint = fields[3]
        timeStamp = fields[4]
        serversDownStr = fields[5]
        statusCodePredicted = fields[6]

        if timeStamp != currentTime:
            endpointStatus = {}
            clientsProcessed = {}
            currentTime = timeStamp

        if apiEndpoint not in endpointStatus:
            endpointStatus[apiEndpoint] = 3 - float(serversDownStr)

        if cliId not in clientsProcessed:
            clientsProcessed[cliId] = True

            if endpointStatus[apiEndpoint] > 0:
                computedStatusCode = "200"
                endpointStatus[apiEndpoint] -= 1
            else:
                computedStatusCode = "500"

            print(f"{reqId} {cliId} {apiEndpoint} {timeStamp} {serversDownStr} {statusCodePredicted} {computedStatusCode}")

