/**************************************************************************************************************************
    Grant other accounts direct access to this share
**************************************************************************************************************************/

use schema ARCHDATA_CIVIC.RAW;
use warehouse ARCHDATA_CIVIC_WH;
use role ACCOUNTADMIN;

/**
Consumer Info needed:
SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();
**/


// ALTER SHARE ARCHDATA_CIVIC_SHARE ADD ACCOUNT = <org_name>.<account_name>;

/**
Consumer Instructions:
* Go to Data Products > Private Sharing.
* Select the Direct Shares tab.
* Click on the share you just sent (it will likely be listed under "Ready to Get").
* Click Get Data.
* They can then choose which Local Database Name to give it and which Roles should have access.
**/