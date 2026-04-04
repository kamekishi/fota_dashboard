from collections import namedtuple
import fotaLogic as FOTALogic
from log import Log
import os
import requests
import select
import sys
import time

if sys.platform == "win32":
    import msvcrt
else:
    import select

class FirmwareLogic:
    rKeyName = 'r'
	
    @staticmethod
    def RunFirmwareCheck(device, imeiScan = False, imeiValidation = False):
        scanInterrupted = False
        
        previousTime = time.time()
        while time.time() - previousTime < 1.0:
            if sys.platform == "win32":
                if msvcrt.kbhit():
                    if FirmwareLogic.rKeyName in msvcrt.getch().decode('utf-8', errors='ignore').lower():
                        scanInterrupted = True
            else:
                if select.select([sys.stdin], [], [], 0)[0]:
                    if FirmwareLogic.rKeyName in sys.stdin.readline().lower():
                        scanInterrupted = True
            
            if scanInterrupted:
                print()
                Log.W('Scan interrupted by user with R key')
                return(0, '', scanInterrupted)
            time.sleep(0.01)
            
        descryptorInfo = FOTALogic.DescriptorInfo().fetch(device.CurrentCustomerCode.modelID, device.IMEI, device.CurrentCustomerCode.currentCountry, device.FirmwareBuild)

        match descryptorInfo.status:
            case 'SUCCESS':
                Log.S(descryptorInfo.status)
                if imeiScan:
                    return (2, device.IMEI, False)
                elif imeiValidation:
                    return (2, '', False)
                else:
                    FirmwareLogic.PrintFirmwareInfo(descryptorInfo)
                    return (2, '', False)
            case 'NO UPDATE AVAILABLE':
                Log.W(descryptorInfo.status)
                return (1, '', False)
            case 'IP BANNED':
                Log.E(descryptorInfo.status)
                print()
                Log.W('Scan interrupted by remote exception')
                return(0, '', True)
            case _:
                Log.E(descryptorInfo.status)
                return (0, '', False)
            
        return (0, '', False)

    @staticmethod
    def PrintFirmwareInfo(descryptorInfo):
        from deviceLogic import DeviceLogic
        Log.Routine('OTA INFO')

        print(f' ---> Base Version <---\n {descryptorInfo.baseVersion.replace('/', '\n ')}\n')
        print(f' ---> Target Version <---\n {descryptorInfo.targetVersion.replace('/', '\n ')}\n {{')
        print(f'     ---> Security {descryptorInfo.securityPatches}')
        print(f'     ---> {descryptorInfo.androidVersion}')
        if descryptorInfo.oneUIVersion is not '?':
            print(f'     ---> {descryptorInfo.oneUIVersion}')
        if descryptorInfo.size is not '?':
            print(f'     ---> {descryptorInfo.size}MB')
        print(' }')

        Log.Routine('OTA DOWNLOAD')

        otaDownloadConfirmation = Log.Confirmation()

        if otaDownloadConfirmation is not None and otaDownloadConfirmation:
            FirmwareLogic.DownloadOTAUpdate(descryptorInfo.downloadURL + '&px-nb=Xero&px-rmtime=Xero', os.path.join(DeviceLogic.DownloadFolderPath, f'{descryptorInfo.baseVersion.split('/')[0]} to {descryptorInfo.targetVersion.split('/')[0]}.zip'))

    @staticmethod
    def DownloadOTAUpdate(downloadURL, outputPackagePath):
        if os.path.exists(outputPackagePath):
            downloadedOverwriteConfirmation = Log.Confirmation('There\'s already an update package with a same name.')

            if downloadedOverwriteConfirmation is not None and downloadedOverwriteConfirmation:
                print()
            else:
                return

        try:
            headers = {
                'User-Agent': 'PostmanRuntime/7.36.1',
                'Cache-Control': 'no-cache'
            }

            with requests.get(downloadURL, headers = headers, stream = True, timeout = 300) as response:
                response.raise_for_status()

                totalBytes = int(response.headers.get('content-length', 0))
                totalRead = 0
                mbSubdivision = 1048576
                lastUpdatePercent = -1

                with open(outputPackagePath, 'wb') as fileStream:
                    for chunk in response.iter_content(chunk_size = 8192):
                        if chunk:
                            fileStream.write(chunk)
                            totalRead += len(chunk)

                            if totalBytes > 0:
                                percent = int((totalRead / totalBytes) * 100)

                                if percent % 5 == 0 and percent != lastUpdatePercent:
                                    mbRead = totalRead // mbSubdivision
                                    packageSize = totalBytes // mbSubdivision
                                    barLength = 10
                                    filled = percent // barLength
                                    bar = f'{'■' * filled}{'□' * (barLength - filled)}'

                                    print(f' [{bar}] {percent}% ({mbRead}MB of {packageSize}MB)')
                                    lastUpdatePercent = percent

                Log.Routine('DOWNLOAD COMPLETED')
        except Exception as ex:
            Log.E(f'Download error: {ex}')
