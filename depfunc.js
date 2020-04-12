const SQL_TOTAL = "SELECT count(*) AS recordCount from latest_data";

const SQL_AUTORENEW = "select PMIAutoRenewStatus, count(*) as PMIAutoRenewStatusCount from latest_data group by PMIAutoRenewStatus;";

const SQL_CHAPTER_AUTORENEW = "select ChapterAutoRenewStatus, count(*) from latest_data group by ChapterAutoRenewStatus;";
//const SQL_JOIN_DATE = "select JoinDate, count(*) from latest_data group by JoinDate;";
const SQL_JOIN_DATE = "select JoinDate, count(*) as JoinDateCount from latest_data group by JoinDate order by JoinDateCount desc;";
const SQL_MEMBER_CLASS = "select MemberClass, count(*) from latest_data group by MemberClass;";

const SQL_DESIGNATION = "select designation, count(*) from latest_data group by Designation;";
const SQL_DESIGNATION_DATA = "select Designation from latest_data;";
const SQL_DESIGNATION_BLANK = "select count(*) DesignationBlank from latest_data where Designation = '';";

const SQL_PGMP = "select count(*) AS PGMP from latest_data where Designation like '%PgMP%';";
const SQL_PMP = "select count(*) AS PMP from latest_data where Designation like '%PMP%';";
const SQL_PMI_SP = "select count(*) AS PMISP from latest_data where Designation like '%PMI-SP%';";
const SQL_PMI_PBA = "select count(*) AS PMIPBA from latest_data where Designation like '%PMI-PBA%';";
const SQL_PMI_RMP = "select count(*) AS PMI_RMP from latest_data where Designation like '%PMI_RMP%';";
const SQL_PMI_ACP = "select count(*) PMI_ACP from latest_data where Designation like '%PMI_ACP%';";

const SQL_EXPIRATION_DATE = "select ExpirationDate, count(*) as ExpirationDateCount from latest_data group by ExpirationDate order by ExpirationDateCount desc;";

const SQL_TITLE = "select Title, count(*) as TitleCount from latest_data group by Title order by TitleCount desc;";
const SQL_WTITLE = "select WTitle, count(*) as WTitleCount from latest_data group by WTitle order by WTitleCount desc;";

const SQL_COMPANY = "select Company, count(*) as CompanyCount from latest_data group by Company order by CompanyCount desc;";
const SQL_WCOMPANY = "select WCompany, count(*) as WCompanyCount from latest_data group by WCompany order by WCompanyCount desc;";

const SQL_CITY = "select City, count(*) as CityCount from latest_data group by City order by CityCount desc;";
const SQL_HCITY = "select HCity, count(*) as HCityCount from latest_data group by HCity order by HCityCount desc;";
const SQL_WCITY = "select WCity, count(*) as WCityCount from latest_data group by WCity order by WCityCount desc;";

const SQL_STATE = "select State, count(*) as StateCount from latest_data group by State order by StateCount desc;";
const SQL_HSTATE = "select HState, count(*) as HStateCount from latest_data group by HState order by HStateCount desc;";
const SQL_WSTATE = "select WState, count(*) as WStateCount from latest_data group by WState order by WStateCount desc;";

const SQL_ZIP5 = "select substr(Zip,1,5) as Zip5, count(*) as Zip5Count from latest_data where length(zip) >= 5 group by Zip5 order by Zip5Count desc;";
const SQL_ZIP = "select Zip, count(*) as ZipCount from latest_data group by Zip order by ZipCount desc;";
const SQL_HZIP = "select HZip, count(*) as HZipCount from latest_data group by HZip order by HZipCount desc;";
const SQL_WZIP = "select WZip, count(*) as WZipCount from latest_data group by WZip order by WZipCount desc;";

const SQL_PMIJOINDATE = "select PMIJoinDate, count(*) as PMIJoinDateCount from latest_data group by PMIJoinDate order by PMIJoinDateCount DESC;";
const SQL_PMIJOINDATE_YEAR = "select substr(PMIJoinDate, 8) as PMIJoinDateYear, count(*) as PMIJoinDateYearCount from latest_data group by PMIJoinDateYear order by PMIJoinDateYearCount DESC;";

const SQL_PMIEXPIRATIONDATE = "select PMIExpirationDate, count(*) as PMIExpirationDateCount from latest_data group by PMIExpirationDate order by PMIExpirationDateCount DESC;";
const SQL_PMIEXPIRATIONDATE_YEAR = "select substr(PMIExpirationDate, 8) as PMIExpirationDateYear, count(*) as PMIExpirationDateCount from latest_data group by PMIExpirationDateYear order by PMIExpirationDateCount DESC;";

const SQL_PMPDATE = "select PMPDate, count(*) as PMPDateCount from latest_data group by PMPDate order by PMPDateCount DESC;";
const SQL_PMPDATE_YEAR = "select substr(PMPDate, 8) as PMPDateYear, count(*) as PMPDateCount from latest_data group by PMPDateYear order by PMPDateCount DESC;";

const SQL_CHAPTERCOUNT = "select ChapterCount, count(*) as ChapterCountCount from latest_data group by ChapterCount order by ChapterCountCount DESC;";
const SQL_SIGSCOUNT = "select SIGsCount, count(*) as SIGsCountCount from latest_data group by SIGsCount order by SIGsCountCount DESC;";

const SQL_INDUSTRYCODES = "select IndustryCodes from latest_data where length(IndustryCodes) > 0;";
const SQL_OCCUPATIONCODES = "select OccupationCodes from latest_data where length(OccupationCodes) > 0;";

let db;

runQuery = function(query) {
	return new Promise(function (resolve, reject) {
		db.all(query, function(err, rows) {
			if (err) {
				reject(err);
			} else {
				resolve(rows);
			}
		})
	})
}

getTotal = function() { return(runQuery(SQL_TOTAL)); };
getAutorenew = function() { return(runQuery(SQL_AUTORENEW)); };
getChapter_autorenew = function() { return(runQuery(SQL_CHAPTER_AUTORENEW)); };
// getJoin_date = function() { return(runQuery(SQL_JOIN_DATE)); };
getJoin_date = function() { return(runQuery(SQL_JOIN_DATE)); };
getMember_class = function() { return(runQuery(SQL_MEMBER_CLASS)); };

getDesignation = function() { return(runQuery(SQL_DESIGNATION)); };
getDesignation_data = function() { return(runQuery(SQL_DESIGNATION_DATA)); };
getDesignation_blank = function() { return(runQuery(SQL_DESIGNATION_BLANK)); };

getPgmp = function() { return(runQuery(SQL_PGMP)); };
getPmp = function() { return(runQuery(SQL_PMP)); };
getPmi_sp = function() { return(runQuery(SQL_PMI_SP)); };
getPmi_pba = function() { return(runQuery(SQL_PMI_PBA)); };
getPmi_rmp = function() { return(runQuery(SQL_PMI_RMP)); };
getPmi_acp = function() { return(runQuery(SQL_PMI_ACP)); };

getExpiration_date = function() { return(runQuery(SQL_EXPIRATION_DATE)); };

getTitle = function() { return(runQuery(SQL_TITLE)); };
getWtitle = function() { return(runQuery(SQL_WTITLE)); };

getCompany = function() { return(runQuery(SQL_COMPANY)); };
getWcompany = function() { return(runQuery(SQL_WCOMPANY)); };

getCity = function() { return(runQuery(SQL_CITY)); };
getHcity = function() { return(runQuery(SQL_HCITY)); };
getWcity = function() { return(runQuery(SQL_WCITY)); };

getState = function() { return(runQuery(SQL_STATE)); };
getHstate = function() { return(runQuery(SQL_HSTATE)); };
getWstate = function() { return(runQuery(SQL_WSTATE)); };

getZip5 = function() { return(runQuery(SQL_ZIP5)); };
getZip = function() { return(runQuery(SQL_ZIP)); };
getHzip = function() { return(runQuery(SQL_HZIP)); };
getWzip = function() { return(runQuery(SQL_WZIP)); };

getPmijoindate = function() { return(runQuery(SQL_PMIJOINDATE)); };
getPmijoindate_year = function() { return(runQuery(SQL_PMIJOINDATE_YEAR)); };

getPmiexpirationdate = function() { return(runQuery(SQL_PMIEXPIRATIONDATE)); };
getPmiexpirationdate_year = function() { return(runQuery(SQL_PMIEXPIRATIONDATE_YEAR)); };

getPmpdate = function() { return(runQuery(SQL_PMPDATE)); };
getPmpdate_year = function() { return(runQuery(SQL_PMPDATE_YEAR)); };

getChaptercount = function() { return(runQuery(SQL_CHAPTERCOUNT)); };
getSigscount = function() { return(runQuery(SQL_SIGSCOUNT)); };

getIndustrycodes = function() { return(runQuery(SQL_INDUSTRYCODES)); };
getOccupationcodes = function() { return(runQuery(SQL_OCCUPATIONCODES)); };

module.exports.report = function(dbIn) {
	db = dbIn;
	return new Promise(function (resolve, reject) {
		let responseData = { };

		getTotal().then((result) => {
	    	responseData.TotalRecordCount = result[0].recordCount;
	    	getAutorenew().then((result) => { responseData.Autorenew = result;
				getChapter_autorenew().then((result) => { responseData.Chapter_autorenew = result;
					getJoin_date().then((result) => { responseData.Join_date = result;
						getMember_class().then((result) => { responseData.Member_class = result;
							getDesignation().then((result) => { responseData.Designation = result;
								getDesignation_data().then((result) => { responseData.Designation_data = result;
									getDesignation_blank().then((result) => { responseData.Designation_blank = result;
										getPgmp().then((result) => { responseData.Pgmp = result;
											getPmp().then((result) => { responseData.Pmp = result;
												getPmi_sp().then((result) => { responseData.Pmi_sp = result;
													getPmi_pba().then((result) => { responseData.Pmi_pba = result;
														getPmi_rmp().then((result) => { responseData.Pmi_rmp = result;
															getPmi_acp().then((result) => { responseData.Pmi_acp = result;
																getExpiration_date().then((result) => { responseData.Expiration_date = result;
																	getTitle().then((result) => { responseData.Title = result;
																		getWtitle().then((result) => { responseData.Wtitle = result;
																			getCompany().then((result) => { responseData.Company = result;
																				getWcompany().then((result) => { responseData.Wcompany = result;
																					getCity().then((result) => { responseData.City = result;
																						getHcity().then((result) => { responseData.Hcity = result;
																							getWcity().then((result) => { responseData.Wcity = result;
																								getState().then((result) => { responseData.State = result;
																									getHstate().then((result) => { responseData.Hstate = result;
																										getWstate().then((result) => { responseData.Wstate = result;
																											getZip5().then((result) => { responseData.Zip5 = result;
																												getZip().then((result) => { responseData.Zip = result;
																													getHzip().then((result) => { responseData.Hzip = result;
																														getWzip().then((result) => { responseData.Wzip = result;
																															getPmijoindate().then((result) => { responseData.Pmijoindate = result;
																																getPmijoindate_year().then((result) => { responseData.Pmijoindate_year = result;
																																	getPmiexpirationdate().then((result) => { responseData.Pmiexpirationdate = result;
																																		getPmiexpirationdate_year().then((result) => { responseData.Pmiexpirationdate_year = result;
																																			getPmpdate().then((result) => { responseData.Pmpdate = result;
																																				getPmpdate_year().then((result) => { responseData.Pmpdate_year = result;
																																					getChaptercount().then((result) => { responseData.Chaptercount = result;
																																						getSigscount().then((result) => { responseData.Sigscount = result;
																																							getIndustrycodes().then((result) => { responseData.Industrycodes = result;
																																								getOccupationcodes().then((result) => { responseData.Occupationcodes = result;
																																									resolve(responseData);
																																								});
																																							});
																																						});
																																					});
																																				});
																																			});
																																		});
																																	});
																																});
																															});
																														});
																													});
																												});
																											});
																										});
																									});
																								});
																							});
																						});
																					});
																				});
																			});
																		});
																	});
																});
															});
														});
													});
												});
											});
										});
									});
								});
							});
						});
					});
				});
	    	});
	    });
	});
};