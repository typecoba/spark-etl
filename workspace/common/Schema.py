class Schema:
    pre_adbrix_attr = [  
      '"ADBRIX"                                      AS tracking',
      'trackingObject.package_name[0]                AS trackingId',      
      'CASE WHEN trackingObject.activity[0] = "search"  THEN "TPD001" \
            WHEN trackingObject.activity[0] = "product_view"  THEN "TPD002" \
            WHEN trackingObject.activity[0] = "add_to_cart" THEN "TPD003" \
            WHEN trackingObject.activity[0] = "purchase" THEN "TPD004" \
            WHEN trackingObject.activity[0] = "sign_up" THEN "TPD012" \
            WHEN trackingObject.activity[0] = "refund" THEN "TPD015" \
            WHEN trackingObject.activity[0] = "add_to_wishlist" THEN "TPD014" \
            WHEN trackingObject.activity[0] = "login" THEN "TPD013" ELSE "TPD999" END  AS trackingEventCode',
      'CASE WHEN trackingObject.platform[0] = "android" THEN trackingObject.gaid[0] ELSE trackingObject.ifa[0] END AS cid',
      'CASE WHEN trackingObject.platform[0] = "android" THEN "ATC001" \
            WHEN trackingObject.platform[0] = "ios" THEN "ATC002" ELSE "ATC999" END AS mosTypeCode', 
      'CAST(logTimeStamp AS string) AS logTimeStamp',
      'CAST(trackingObject.event_time[0] * 1000 AS String)    AS eventTimeStamp',
      '""                                            as campaign',
      'trackingObject.product_id[0]                  AS contentId',
      'trackingObject.product_name[0]                AS contentName',
      'trackingObject.price[0]                       AS value',
      'trackingObject.quantity[0]                    AS quantity',
      'trackingObject.sales[0]                       AS amount',
      'trackingObject.currency[0]                    AS currency',
      'trackingObject.activity_param[0]              AS activityParam',
      '""                                            AS attributed',
      '""                                            AS latdAdvertisingPartnerName',      
    ]
    pre_branch_attr = [
      '"BRANCH"                                        AS tracking',
      'trackingObject.organization_name[0]             AS trackingId',
      'CASE WHEN trackingObject.name[0] = "SEARCH"   THEN "TPD007" \
            WHEN trackingObject.name[0] = "VIEW_ITEM"  THEN "TPD002" \
            WHEN trackingObject.name[0] = "ADD_TO_CART" THEN "TPD003" \
            WHEN trackingObject.name[0] = "PURCHASE" THEN "TPD004"\
            WHEN trackingObject.name[0] = "INSTALL"    THEN "TPD008"\
            WHEN trackingObject.name[0] = "OPEN" THEN "TPD009"  ELSE "TPD999" END     AS trackingEventCode',
      'CASE WHEN trackingObject.ud_os[0] = "ANDROID" THEN trackingObject.ud_aaid[0] \
            WHEN trackingObject.ud_os[0] = "IOS" THEN trackingObject.ud_idfa[0] END                     AS cid',
      'CASE WHEN trackingObject.ud_os[0] = "ANDROID" THEN "ATC001" \
            WHEN trackingObject.ud_os[0] = "IOS" THEN "ATC002" ELSE "ATC999" END      AS mosTypeCode',
      'logTimeStamp                  ',
      'trackingObject.event_timestamp[0]               AS eventTimeStamp',
      'trackingObject.latd_campaign[0]                 AS campaign',
      'trackingObject.ci0_product_name[0]              AS contentId',
      '""                                              AS contentName',
      'trackingObject.ci0_price[0]                     AS value',
      'trackingObject.ci0_quantity[0]                  AS quantity',
      '"0"                                             AS amount',
      'trackingObject.ci0_currency[0]                  AS currency',
      '""                                              AS activityParam',
      'trackingObject.attributed[0]                    AS attributed',
      'trackingObject.latd_advertising_partner_name[0] AS latdAdvertisingPartnerName',
    ]
    pre_singular_attr = [
      '"SINGULAR"                                               AS tracking',
      'trackingObject.app_name[0]                               AS trackingid',
      'CASE WHEN trackingObject.evtname[0] = "ViewItem"  THEN "TPD002"\
            WHEN trackingObject.evtname[0] = "AddToCart" THEN "TPD003"\
            WHEN trackingObject.evtname[0] = "Purchase"  THEN "TPD004"\
            WHEN trackingObject.evtname[0] = "FirstPurchase"  THEN "TPD010"\
            WHEN trackingObject.evtname[0] = "__START__" THEN "TPD008"\
            WHEN trackingObject.evtname[0] = "__SESSION__"  THEN "TPD009"\
            WHEN trackingObject.evtname[0] = "__iap__"   THEN "TPD011"   ELSE "TPD999" END AS trackingEventCode',
      'CASE WHEN trackingObject.platform[0] = "Android" THEN trackingObject.aifa[0] ELSE trackingObject.idfa[0] END AS cid',
      'CASE WHEN trackingObject.platform[0] = "Android" THEN "ATC001"\
            WHEN trackingObject.platform[0] = "iOS" THEN "ATC002"  ELSE "ATC999" END  AS mosTypeCode',
      'logTimeStamp',
      'CAST(trackingObject.utc[0] * 1000 AS String)             AS eventTimeStamp',
      'trackingObject.cid[0]                                    AS campaign',
      'CASE WHEN trackingObject.evtname[0] = "__iap__" THEN GET_JSON_OBJECT(trackingObject.evtattr[0], "$[0].id") ELSE GET_JSON_OBJECT(trackingObject.evtattr[0], "$[0].prd_code") END AS contentId',
      'GET_JSON_OBJECT(trackingObject.evtattr[0], "$[0].product")  AS contentName',
      'CASE WHEN trackingObject.evtname[0] = "__iap__" THEN GET_JSON_OBJECT(trackingObject.evtattr[0], "$[0].price") ELSE "0" END  AS value',
      'CASE WHEN trackingObject.evtname[0] = "__iap__" THEN GET_JSON_OBJECT(trackingObject.evtattr[0], "$[0].quantity") ELSE "0" END AS quantity',
      'trackingObject.amount[0]                                 as amount',
      'trackingObject.currency[0]                               as currency',
      'trackingObject.evtattr[0]                                AS activityParam',
      'trackingObject.is_re_eng[0]                              AS attributed',
      '""                                                       AS latdAdvertisingPartnerName',
    ]

    # preprocess union 후 처리
    pre_union_attr = [
      'now() as processDate', 
      'hour(now()) as processHour', 
      'NVL(LEAD(eventTimeStamp) OVER (PARTITION BY tracking, trackingId, cid, mosTypeCode ORDER BY tracking ASC, trackingId ASC, cid ASC, eventTimeStamp ASC), "") AS nextEventTimeStamp',
      '*'  
    ]