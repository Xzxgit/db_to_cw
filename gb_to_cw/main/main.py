import json
import logging
import time
import pyodbc
from k3cloud_webapi_sdk.main import K3CloudApiSdk


# 连接字符串
def get_db_connect():
    server = '192.168.6.10'
    database = 'Data_ZY'
    username = 'sa'
    password = 'yfdz_0928'
    connection_string = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
    )

    # 建立连接
    conn = pyodbc.connect(connection_string)

    # 上一次查询的最大 xh（表的主键自增列为 xh ）
    last_max_id = 0

    while True:
        # 查询新数据
        # query = f"-- SELECT * FROM czjl WHERE ywlx = '销售出厂' ORDER BY xh ASC"
        query = f"SELECT [xh], [ywlx], [pm], [mz], [pz], [jz], [fhdw], [shdw], [sfyx] FROM czjl where sfyx = '有效'"
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        # 处理新数据
        if rows:
            print(f"发现 {len(rows)} 条新数据：")
            for row in rows:
                print(row)
                last_max_id = row.xh  # 更新最大 ID
        else:
            print("没有新订单。")

        # 等待一段时间后再次查询
        time.sleep(5)  # 每 5 秒查询一次

        # # 关闭连接
        # cursor.close()
        # conn.close()


def selling_order_save():
    # 注意 1：此处不再使用参数形式传入用户名及密码等敏感信息，改为在登录配置文件中设置。
    # 注意 2：必须先配置第三方系统登录授权信息后，再进行业务操作，详情参考各语言版本SDK介绍中的登录配置文件说明。
    # 读取配置，初始化SDK
    api_sdk = K3CloudApiSdk("http://42.234.203.233:8090/K3Cloud")
    api_sdk.Init(config_path='../conf.ini', config_node='config')
    # 请求参数
    para = {
        "Model": {
            "FBillTypeID": {
                "FNUMBER": ""
            },
            "FDate": "1999-01-01",
            "FSaleOrgId": {
                "FNumber": ""
            },
            "FCustomerID": {    # 客户
                "FNumber": ""
            },
            # "FCarriageNO": "",  # 运输单号，非必填
            # "FSalesManID": {
            #     "FNumber": ""
            # }, # 销售人员，非必填
            "FStockOrgId": {     # 发货单位，默认鑫源铸业，也可不填
                "FNumber": "鑫源铸业"
            },
            # "FReceiveAddress": "",  # 收货地址，非必填
            # "FReceiverContactID": {   # 收货方联系人，非必填
            #     "FNAME": ""
            # },
            # "FOwnerTypeIdHead": "",  # 货主类型，非必填
            # "FOwnerIdHead": {          # 货主，非必填
            #     "FNumber": ""
            # },
            # "FScanBox": "",
            # "FGenFromPOS_CMK": "false",
            # "FCDateOffsetUnit": "",
            # "FCDateOffsetValue": 0,
            # "FPlanRecAddress": "",
            # "FIsTotalServiceOrCost": "false",
            # "FSHOPNUMBER": "",
            # "FGYDATE": "1900-01-01",
            # "FSALECHANNEL": "",
            # "FLogisticsNos": "",
            "SubHeadEntity": {
                "FSettleCurrID": {   # 结算币别
                    "FNumber": ""
                },
                # "FThirdBillNo": "",
                # "FThirdBillId": "",
                # "FThirdSrcType": "",
                "FSettleOrgID": {   # 结算组织
                    "FNumber": ""
                },
                # "FIsIncludedTax": "false", # 是否含税，，非必填
                # "FReceiverAddress": "",   # 收货人地址，非必填
                # "FReceiverName": "",      # 收货人，非必填
                # "FReceiverMobile": "",    # 收货人手机，非必填
                # "FReceiverCountry": "",   # 收货人国家，非必填
                # "FReceiverState": "",     # 收货人省份，非必填
                # "FReceiverCity": "",      # 收货人城市，非必填
                # "FReceiverDistrict": "",  # 收货人地区，非必填
                # "FReceiverPhone": "",     # 收货人电话，非必填
                # "FAllDisCount": 0,        # 整单折扣额，非必填
            },
            "FEntity": [
                {
                    # "FENTRYID": 0,
                    # "FRowType": "",
                    # "FCustMatID": {       # 客户物料编码，非必填
                    #     "FNumber": ""
                    # },
                    "FMaterialID": {        # 物料编码
                        "FNumber": ""
                    },
                    "FUnitID": {            # 库存单位
                        "FNumber": ""
                    },
                    # "FInventoryQty": 0,   # 当前库存，非必填
                    # "FRealQty": 0,        # 实发数量，非必填
                    # "FPrice": 0,          # 单价，非必填
                    # "FTaxPrice": 0,       # 含税单价，非必填
                    "FOwnerTypeID": "个人",  # 货主类型
                    "FOwnerID": {
                        "FNumber": ""
                    },
                    "FActQty": 0,
                    "FSalUnitID": {
                        "FNumber": ""
                    },
            #         "FSALUNITQTY": 0,
            #         "FSALBASEQTY": 0,
            #         "FPRICEBASEQTY": 0,
            #         "FProjectNo": "",
            #         "FOUTCONTROL": "false",
            #         "FRepairQty": 0,
            #         "FIsCreateProDoc": "",
            #         "FEOwnerSupplierId": {
            #             "FNUMBER": ""
            #         },
            #         "FIsOverLegalOrg": "false",
            #         "FESettleCustomerId": {
            #             "FNUMBER": ""
            #         },
            #         "FPriceListEntry": {
            #             "FNUMBER": ""
            #         },
            #         "FARNOTJOINQTY": 0,
            #         "FQmEntryID": 0,
            #         "FConvertEntryID": 0,
            #         "FSOEntryId": 0,
            #         "FRetailSaleProm": "false",
            #         "FBeforeDisPriceQty": 0,
            #         "FSignQty": 0,
            #         "FCheckDelivery": "false",
            #         "FThirdEntryId": "",
            #         "FETHIRDBILLID": "",
            #         "FETHIRDBILLNO": "",
            #         "FAllAmountExceptDisCount": 0,
            #         "FSettleBySon": "false",
            #         "FBOMEntryId": 0,
            #         "FGYENTERTIME": "1900-01-01",
            #         "FMaterialID_Sal": {
            #             "FNUMBER": ""
            #         },
            #         "FInStockBillno": "",
            #         "FInStockEntryId": 0,
            #         "FReceiveBillno": "",
            #         "FReceiveEntryId": 0,
            #         "FIsReplaceOut": "false",
            #         "FReplaceMaterialID": {
            #             "FNUMBER": ""
            #         },
            #         "FRowARStatus": "",
            #         "FVmiBusinessStatus": "false",
            #         "FTaxDetailSubEntity": [
            #             {
            #                 "FDetailID": 0,
            #                 "FTaxRate": 0
            #             }
            #         ],
            #         "FSerialSubEntity": [
            #             {
            #                 "FDetailID": 0,
            #                 "FSerialNo": "",
            #                 "FSerialNote": ""
            #             }
            #         ]
            #     }
            # ],
            # "FOutStockTrace": [
            #     {
            #         "FEntryID": 0,
            #         "FLogComId": {
            #             "FCODE": ""
            #         },
            #         "FCarryBillNo": "",
            #         "FPhoneNumber": "",
            #         "FFrom": "",
            #         "FTo": "",
            #         "FDelTime": "1900-01-01",
            #         "FTraceStatus": "",
            #         "FReceiptTime": "1900-01-01",
            #         "FCarryBillNoType": "",
            #         "FOutStockTraceDetail": [
            #             {
            #                 "FDetailID": 0,
            #                 "FTraceTime": "",
            #                 "FTraceDetail": ""
            #             }
            #         ]
                }
            ]
        }
    }
    # 业务对象标识
    form_id = "SAL_OUTSTOCK"
    # 调用接口
    response = api_sdk.Save(form_id, para)

    print("接口返回结果：" + response)
    # 对返回结果进行解析和校验
    res = json.loads(response)
    if res["Result"]["ResponseStatus"]["IsSuccess"]:
        return True
    else:
        logging.error(res)
        return False


def buying_order_save():
    # 注意 1：此处不再使用参数形式传入用户名及密码等敏感信息，改为在登录配置文件中设置。
    # 注意 2：必须先配置第三方系统登录授权信息后，再进行业务操作，详情参考各语言版本SDK介绍中的登录配置文件说明。
    # 读取配置，初始化SDK
    api_sdk = K3CloudApiSdk('http://42.234.203.233:8090/K3Cloud')
    api_sdk.Init(config_path='../conf.ini', config_node='config')
    # 请求参数
    para = {
      "NeedUpDateFields": [],
      "NeedReturnFields": [],
      "IsDeleteEntry": "true",
      "SubSystemId": "",
      "IsVerifyBaseDataField": "false",
      "IsEntryBatchFill": "true",
      "ValidateFlag": "true",
      "NumberSearch": "true",
      "IsAutoAdjustField": "false",
      "InterationFlags": "",
      "IgnoreInterationFlag": "",
      "IsControlPrecision": "false",
      "ValidateRepeatJson": "false",
      "Model": {
        "FID": 0,
        "FBillTypeID": {
          "FNUMBER": ""
        },
        "FBusinessType": "",
        "FBillNo": "",
        "FDate": "1900-01-01",
        "FStockOrgId": {
          "FNumber": ""
        },
        "FStockDeptId": {
          "FNumber": ""
        },
        "FStockerGroupId": {
          "FNumber": ""
        },
        "FStockerId": {
          "FNumber": ""
        },
        "FDemandOrgId": {
          "FNumber": ""
        },
        "FCorrespondOrgId": {
          "FNumber": ""
        },
        "FPurchaseOrgId": {
          "FNumber": ""
        },
        "FPurchaseDeptId": {
          "FNumber": ""
        },
        "FPurchaserGroupId": {
          "FNumber": ""
        },
        "FPurchaserId": {
          "FNumber": ""
        },
        "FSupplierId": {
          "FNumber": ""
        },
        "FSupplyId": {
          "FNumber": ""
        },
        "FSupplyAddress": "",
        "FSettleId": {
          "FNumber": ""
        },
        "FChargeId": {
          "FNumber": ""
        },
        "FOwnerTypeIdHead": "",
        "FOwnerIdHead": {
          "FNumber": ""
        },
        "FConfirmerId": {
          "FUserID": ""
        },
        "FConfirmDate": "1900-01-01",
        "FScanBox": "",
        "FCDateOffsetUnit": "",
        "FCDateOffsetValue": 0,
        "FProviderContactID": {
          "FCONTACTNUMBER": ""
        },
        "FSplitBillType": "",
        "FSupplyEMail": "",
        "FSalOutStockOrgId": {
          "FNumber": ""
        },
        "FInStockFin": {
          "FEntryId": 0,
          "FSettleOrgId": {
            "FNumber": ""
          },
          "FSettleTypeId": {
            "FNumber": ""
          },
          "FPayConditionId": {
            "FNumber": ""
          },
          "FSettleCurrId": {
            "FNumber": ""
          },
          "FIsIncludedTax": "false",
          "FPriceTimePoint": "",
          "FPriceListId": {
            "FNumber": ""
          },
          "FDiscountListId": {
            "FNumber": ""
          },
          "FLocalCurrId": {
            "FNumber": ""
          },
          "FExchangeTypeId": {
            "FNumber": ""
          },
          "FExchangeRate": 0,
          "FISPRICEEXCLUDETAX": "false",
          "FAllDisCount": 0,
          "FHSExchangeRate": 0
        },
        "FInStockEntry": [
          {
            "FEntryID": 0,
            "FRowType": "",
            "FWWInType": "",
            "FMaterialId": {
              "FNumber": ""
            },
            "FUnitID": {
              "FNumber": ""
            },
            "FMaterialDesc": "",
            "FAuxPropId": {},
            "FParentMatId": {
              "FNUMBER": ""
            },
            "FWWPickMtlQty": 0,
            "FRealQty": 0,
            "FPriceUnitID": {
              "FNumber": ""
            },
            "FPrice": 0,
            "FLot": {
              "FNumber": ""
            },
            "FTaxCombination": {
              "FNumber": ""
            },
            "FStockId": {
              "FNumber": ""
            },
            "FDisPriceQty": 0,
            "FStockLocId": {},
            "FStockStatusId": {
              "FNumber": ""
            },
            "FMtoNo": "",
            "FGiveAway": "false",
            "FNote": "",
            "FProduceDate": "1900-01-01",
            "FOWNERTYPEID": "",
            "FExtAuxUnitId": {
              "FNumber": ""
            },
            "FExtAuxUnitQty": 0,
            "FCheckInComing": "false",
            "FProjectNo": "",
            "FIsReceiveUpdateStock": "false",
            "FInvoicedJoinQty": 0,
            "FPriceBaseQty": 0,
            "FSetPriceUnitID": {
              "FNumber": ""
            },
            "FRemainInStockUnitId": {
              "FNumber": ""
            },
            "FBILLINGCLOSE": "false",
            "FRemainInStockQty": 0,
            "FAPNotJoinQty": 0,
            "FRemainInStockBaseQty": 0,
            "FTaxPrice": 0,
            "FEntryTaxRate": 0,
            "FDiscountRate": 0,
            "FCostPrice": 0,
            "FBOMId": {
              "FNumber": ""
            },
            "FSupplierLot": "",
            "FExpiryDate": "1900-01-01",
            "FAuxUnitQty": 0,
            "FOWNERID": {
              "FNumber": ""
            },
            "FSRCBILLTYPEID": "",
            "FSRCBillNo": "",
            "FAllotBaseQty": 0,
            "FIsScanEntry": "false",
            "FAllAmountExceptDisCount": 0,
            "FPriceDiscount": 0,
            "FConsumeSumQty": 0,
            "FBaseConsumeSumQty": 0,
            "FRejectsDiscountAmount": 0,
            "FSalOutStockBillNo": "",
            "FSalOutStockEntryId": 0,
            "FBeforeDisPriceQty": 0,
            "FPayableEntryID": 0,
            "FSUBREQBILLNO": "",
            "FSUBREQBILLSEQ": 0,
            "FSUBREQENTRYID": 0,
            "FEntryPruCost": [
              {
                "FDetailID": 0
              }
            ],
            "FTaxDetailSubEntity": [
              {
                "FDetailID": 0,
                "FTaxRate": 0
              }
            ],
            "FSerialSubEntity": [
              {
                "FDetailID": 0,
                "FSerialNo": "",
                "FSerialNote": ""
              }
            ]
          }
        ]
      }
    }
    # 业务对象标识
    form_id = "STK_InStock"
    # 调用接口
    response = api_sdk.Save(form_id , para)

    print("接口返回结果：" + response)
    # 对返回结果进行解析和校验
    res = json.loads(response)
    if res["Result"]["ResponseStatus"]["IsSuccess"]:
        return True
    else:
        logging.error(res)
        return False


if __name__ == '__main__':
    # get_db_connect()
    selling_order_save()
    # buying_order_save()