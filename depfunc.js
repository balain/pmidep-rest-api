const auth = require('./auth.js');
const config = require('./config.js');
const file = config.dbFilename;

const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database(file);

//*******************************************************************************
const SQL_TOTAL = "SELECT count(*) AS recordCount from latest_data";
const SQL_DATADATE = "SELECT DataDate from latest_data group by DataDate limit 1;";

const SQL_AUTORENEW =
	"select PMIAutoRenewStatus, count(*) as PMIAutoRenewStatusCount from latest_data group by PMIAutoRenewStatus;";

const SQL_CHAPTER_AUTORENEW =
	"select ChapterAutoRenewStatus, count(*) from latest_data group by ChapterAutoRenewStatus;";
//const SQL_JOIN_DATE = "select JoinDate, count(*) from latest_data group by JoinDate;";
const SQL_JOIN_DATE =
	"select JoinDate, count(*) as JoinDateCount from latest_data group by JoinDate order by JoinDateCount desc;";
const SQL_MEMBER_CLASS =
	"select MemberClass, count(*) from latest_data group by MemberClass;";

const SQL_DESIGNATION =
	"select designation, count(*) from latest_data group by Designation;";
const SQL_DESIGNATION_DATA = "select Designation from latest_data;";
const SQL_DESIGNATION_BLANK =
	"select count(*) DesignationBlank from latest_data where Designation = '';";

const SQL_PMP =
	"select count(*) AS PMP from latest_data where Designation like '%PMP%';";
const SQL_PGMP =
	"select count(*) AS PGMP from latest_data where Designation like '%PgMP%';";
const SQL_CAPM =
	"select count(*) AS CAPM from latest_data where Designation like '%CAPM%';";
const SQL_PMI_SP =
	"select count(*) AS PMISP from latest_data where Designation like '%PMI-SP%';";
const SQL_PMI_PBA =
	"select count(*) AS PMIPBA from latest_data where Designation like '%PMI-PBA%';";
const SQL_PMI_RMP =
	"select count(*) AS PMI_RMP from latest_data where Designation like '%PMI_RMP%';";
const SQL_PMI_ACP =
	"select count(*) PMI_ACP from latest_data where Designation like '%PMI_ACP%';";

const SQL_EXPIRATION_DATE =
	"select ExpirationDate, count(*) as ExpirationDateCount from latest_data group by ExpirationDate order by ExpirationDateCount desc;";

const SQL_TITLE =
	"select Title, count(*) as TitleCount from latest_data group by Title order by TitleCount desc;";
const SQL_WTITLE =
	"select WTitle, count(*) as WTitleCount from latest_data group by WTitle order by WTitleCount desc;";

const SQL_COMPANY =
	"select Company, count(*) as CompanyCount from latest_data group by Company order by CompanyCount desc;";
const SQL_WCOMPANY =
	"select WCompany, count(*) as WCompanyCount from latest_data group by WCompany order by WCompanyCount desc;";

const SQL_CITY =
	"select City, count(*) as CityCount from latest_data group by City order by CityCount desc;";
const SQL_HCITY =
	"select HCity, count(*) as HCityCount from latest_data group by HCity order by HCityCount desc;";
const SQL_WCITY =
	"select WCity, count(*) as WCityCount from latest_data group by WCity order by WCityCount desc;";

const SQL_STATE =
	"select State, count(*) as StateCount from latest_data group by State order by StateCount desc;";
const SQL_HSTATE =
	"select HState, count(*) as HStateCount from latest_data group by HState order by HStateCount desc;";
const SQL_WSTATE =
	"select WState, count(*) as WStateCount from latest_data group by WState order by WStateCount desc;";

const SQL_ZIP5 =
	"select substr(Zip,1,5) as Zip5, count(*) as Zip5Count from latest_data where length(zip) >= 5 group by Zip5 order by Zip5Count desc;";
const SQL_ZIP5_TOP5 =
	"select substr(Zip,1,5) as Zip5, count(*) as Zip5Count from latest_data where length(zip) >= 5 group by Zip5 order by Zip5Count desc limit 5;";
const SQL_ZIP =
	"select Zip, count(*) as ZipCount from latest_data group by Zip order by ZipCount desc;";
const SQL_HZIP =
	"select HZip, count(*) as HZipCount from latest_data group by HZip order by HZipCount desc;";
const SQL_WZIP =
	"select WZip, count(*) as WZipCount from latest_data group by WZip order by WZipCount desc;";

const SQL_PMIJOINDATE =
	"select PMIJoinDate, count(*) as PMIJoinDateCount from latest_data group by PMIJoinDate order by PMIJoinDateCount DESC;";
const SQL_PMIJOINDATE_YEAR =
	"select substr(PMIJoinDate, 8) as PMIJoinDateYear, count(*) as PMIJoinDateYearCount from latest_data group by PMIJoinDateYear order by PMIJoinDateYearCount DESC;";

const SQL_PMIEXPIRATIONDATE =
	"select PMIExpirationDate, count(*) as PMIExpirationDateCount from latest_data group by PMIExpirationDate order by PMIExpirationDateCount DESC;";
const SQL_PMIEXPIRATIONDATE_YEAR =
	"select substr(PMIExpirationDate, 8) as PMIExpirationDateYear, count(*) as PMIExpirationDateCount from latest_data group by PMIExpirationDateYear order by PMIExpirationDateCount DESC;";

const SQL_PMPDATE =
	"select PMPDate, count(*) as PMPDateCount from latest_data group by PMPDate order by PMPDateCount DESC;";
const SQL_PMPDATE_YEAR =
	"select substr(PMPDate, 8) as PMPDateYear, count(*) as PMPDateCount from latest_data group by PMPDateYear order by PMPDateCount DESC;";

const SQL_CHAPTERCOUNT =
	"select ChapterCount, count(*) as ChapterCountCount from latest_data group by ChapterCount order by ChapterCountCount DESC;";
const SQL_SIGSCOUNT =
	"select SIGsCount, count(*) as SigsCountCount from latest_data group by SIGsCount order by SigsCountCount DESC;";

const SQL_INDUSTRYCODES =
	"select IndustryCodes, count(*) as IndustryCodesCount from latest_data where length(IndustryCodes) > 0 group by IndustryCodes order by IndustryCodesCount DESC;";
const SQL_OCCUPATIONCODES =
	"select OccupationCodes from latest_data where length(OccupationCodes) > 0;";

// *********************************************************************************

const runQuery = function(caption, query) {
	return new Promise(function(resolve, reject) {
		db.all(query, function(err, rows) {
			if (err) {
				reject(err);
			} else {
				resolve({caption:caption, data:rows});
			}
		});
	});
};

const getTotal = function() {
	return new Promise(function(resolve, reject) {
		db.all(SQL_TOTAL, function(err, rows) {
			if (err) {
				reject(err);
			} else {
				resolve(rows);
			}
		});
	})
};

getDataDate = function() {
	return runQuery("DataDate", SQL_DATADATE);
};

getAutorenew = function() {
	return runQuery("Autorenew", SQL_AUTORENEW);
};

getChapter_autorenew = function() {
	return runQuery("Chapter Autorenew", SQL_CHAPTER_AUTORENEW);
};

getJoin_date = function() {
	return runQuery("Join Date", SQL_JOIN_DATE);
};
getMember_class = function() {
	return runQuery("Member Class", SQL_MEMBER_CLASS);
};

getDesignation = function() {
	return runQuery("Designation", SQL_DESIGNATION);
};

getDesignation_data = function() {
	return runQuery("Designation (data)", SQL_DESIGNATION_DATA);
};

getDesignation_blank = function() {
	return runQuery("Designation (blank)", SQL_DESIGNATION_BLANK);
};

getCapm = function() {
	return runQuery("CAPM", SQL_CAPM);
};

getPgmp = function() {
	return runQuery("PGMP", SQL_PGMP);
};

getPmp = function() {
	return runQuery("PMP", SQL_PMP);
};

getPmi_sp = function() {
	return runQuery("PMI SP", SQL_PMI_SP);
};

getPmi_pba = function() {
	return runQuery("PMI PBA", SQL_PMI_PBA);
};

getPmi_rmp = function() {
	return runQuery("PMI RMP", SQL_PMI_RMP);
};

getPmi_acp = function() {
	return runQuery("PMI ACP", SQL_PMI_ACP);
};

getExpiration_date = function() {
	return runQuery("Expiration Date", SQL_EXPIRATION_DATE);
};

getTitle = function() {
	return runQuery("Title", SQL_TITLE);
};

getWtitle = function() {
	return runQuery("Work Title", SQL_WTITLE);
};

getCompany = function() {
	return runQuery("Company", SQL_COMPANY);
};

getWcompany = function() {
	return runQuery("Work Company", SQL_WCOMPANY);
};

getCity = function() {
	return runQuery("City", SQL_CITY);
};

getHcity = function() {
	return runQuery("Home City", SQL_HCITY);
};

getWcity = function() {
	return runQuery("Work City", SQL_WCITY);
};

getState = function() {
	return runQuery("State", SQL_STATE);
};

getHstate = function() {
	return runQuery("Home State", SQL_HSTATE);
};

getWstate = function() {
	return runQuery("Work State", SQL_WSTATE);
};

getZip5 = function() {
	return runQuery("Zip5", SQL_ZIP5);
};

getZip = function() {
	return runQuery("Zip", SQL_ZIP);
};
getHzip = function() {
	return runQuery("Home Zip", SQL_HZIP);
};
getWzip = function() {
	return runQuery("Work Zip", SQL_WZIP);
};

getPmijoindate = function() {
	return runQuery("PMI Join Date", SQL_PMIJOINDATE);
};
getPmijoindate_year = function() {
	return runQuery("PMI Join Date (Year)", SQL_PMIJOINDATE_YEAR);
};

getPmiexpirationdate = function() {
	return runQuery("PMI Expiration Date", SQL_PMIEXPIRATIONDATE);
};
getPmiexpirationdate_year = function() {
	return runQuery("PMI Expiration Date (Year)", SQL_PMIEXPIRATIONDATE_YEAR);
};

getPmpdate = function() {
	return runQuery("PMP Date", SQL_PMPDATE);
};
getPmpdate_year = function() {
	return runQuery("PMP Date (Year)", SQL_PMPDATE_YEAR);
};

getChaptercount = function() {
	return runQuery("Chapter Count", SQL_CHAPTERCOUNT);
};
getSigscount = function() {
	return runQuery("SIGs Count", SQL_SIGSCOUNT);
};

getIndustrycodes = function() {
	return runQuery("Industry Codes", SQL_INDUSTRYCODES);
};
getOccupationcodes = function() {
	return runQuery("Occupation Codes", SQL_OCCUPATIONCODES);
};

// **************************************************************************
module.exports.closeDb = function() {
	return new Promise(function(resolve, reject) {
		console.log("Caught interrupt signal. Closing database connection...");
		db.close();
		console.log("...done");
		resolve({result:"ok"});
	});
};

module.exports.findId = function(id) {
	return new Promise(function(resolve, reject) {
		var sql = "SELECT * FROM latest_data WHERE ID = '" + id + "'";
		db.all(sql, function(err, rows) {
			if (err) {
				reject(err);
			} else {
		    	resolve({data: rows[0], meta: {rows:rows.length}});
		    }
		});	
	})
};

module.exports.findLastName = function(lastname) {
	return new Promise(function(resolve, reject) {
		var sql = "SELECT ID, FullName, FirstName, LastName, WTitle, PMPNumber, PMPDate, PMIJoinDate, PMIExpirationDate, PMIAutoRenewStatus, ChapterAutoRenewStatus, DataDate FROM latest_data WHERE LastName = '" + lastname + "'";
		console.log(sql);
		db.all(sql, function(err, rows) {
			if (err) {
				console.log(err);
				reject(err);
			} else {
		    	resolve({data: rows, meta: {rows:rows.length}});
		    }
		})
	})
}

const simpleReportFunctions = [	runQuery("Total", SQL_TOTAL), 
                               	runQuery("Member Class", SQL_MEMBER_CLASS),
								runQuery("Zip5 Top 5", SQL_ZIP5_TOP5),
								runQuery("PMI Join Date (Year)", SQL_PMIJOINDATE_YEAR),
								runQuery("PMI Expiration Date (Year)", SQL_PMIEXPIRATIONDATE_YEAR),
								runQuery("PMP Date (Year)", SQL_PMPDATE_YEAR),
								runQuery("Chapter Count", SQL_CHAPTERCOUNT),
								runQuery("SIGs Count", SQL_SIGSCOUNT),
							];

// Force a refresh the first run
let priorReportSimpleDate = new Date(2000, 0, 1);
let priorReportSimpleData = "";
let priorReportDate = new Date(2000, 0, 1);
let priorReportData = "";

module.exports.reportSimple = function() {
	return new Promise(function(resolve, reject) {
		if ((new Date() - priorReportSimple) < (1000*60*60*24)) {
			console.log("Returning cached priorReportSimple");
			resolve(priorReportSimpleData);
		} else {
			console.log("Creating new reportSimple...");

			let results = [];
			let response = {};
			let i = [];

			Promise.all(simpleReportFunctions)
				.then(results => {
					// console.log("results", results);
					// resolve( { results['caption'] = results['data'] });
					results.forEach(function(i) {
						// console.log("i", i);
						if (i.data.length === 1) {
							response[i.caption] = i.data[0];
						} else {
							// Create an object to store the values
							response[i.caption] = {};
							// Collapse the data
							Object.keys(i.data).forEach(function(k) {
								// console.log("keys ", i.data[k], k);
								response[i.caption][Object.values(i.data[k])[0]] = Object.values(i.data[k])[1];
							})
							// response[i.caption] = i.data;
						}
					})
					priorReportSimpleData = response;
					priorReportSimpleDate = new Date();
					resolve(response);
				}).catch(err => {
					console.error(err);
					priorReportSimple = false;
					reject(err);
				})
		}
	})
};

module.exports.report = function() {
	return new Promise(function(resolve, reject) {
		if ((new Date() - priorReportDate) < (1000*60*60*24)) {
			console.log("Returning cached priorReport");
			resolve(priorReportData);
		} else {
			console.log("Creating new report output...");

			let responseData = {};

			getDataDate().then((result) => {
				responseData.DataDate = result.data[0].DataDate;
				console.log("DataDate: ", responseData.DataDate);
			getTotal().then((result) => {
				responseData.TotalRecordCount = result[0].recordCount;
				getAutorenew().then((result) => {
					responseData.Autorenew = result;
					getChapter_autorenew().then((result) => {
						responseData.Chapter_autorenew = result;
						getJoin_date().then((result) => {
							responseData.Join_date = result;
							getMember_class().then((result) => {
								responseData.Member_class = result;
								getDesignation().then((result) => {
									responseData.Designation = result;
									getDesignation_data().then((result) => {
										responseData.Designation_data = result;
										getDesignation_blank().then((result) => {
											responseData.Designation_blank = result;
											getPgmp().then((result) => {
												responseData.Pgmp = result;

											getCapm().then((result) => {
												responseData.Capm = result;

												getPmp().then((result) => {
													responseData.Pmp = result;
													getPmi_sp().then((result) => {
														responseData.Pmi_sp = result;
														getPmi_pba().then(
															(result) => {
																responseData.Pmi_pba = result;
																getPmi_rmp().then(
																	(result) => {
																		responseData.Pmi_rmp = result;
																		getPmi_acp().then(
																			(
																				result
																			) => {
																				responseData.Pmi_acp = result;
																				getExpiration_date().then(
																					(
																						result
																					) => {
																						responseData.Expiration_date = result;
																						getTitle().then(
																							(
																								result
																							) => {
																								responseData.Title = result;
																								getWtitle().then(
																									(
																										result
																									) => {
																										responseData.Wtitle = result;
																										getCompany().then(
																											(
																												result
																											) => {
																												responseData.Company = result;
																												getWcompany().then(
																													(
																														result
																													) => {
																														responseData.Wcompany = result;
																														getCity().then(
																															(
																																result
																															) => {
																																responseData.City = result;
																																getHcity().then(
																																	(
																																		result
																																	) => {
																																		responseData.Hcity = result;
																																		getWcity().then(
																																			(
																																				result
																																			) => {
																																				responseData.Wcity = result;
																																				getState().then(
																																					(
																																						result
																																					) => {
																																						responseData.State = result;
																																						getHstate().then(
																																							(
																																								result
																																							) => {
																																								responseData.Hstate = result;
																																								getWstate().then(
																																									(
																																										result
																																									) => {
																																										responseData.Wstate = result;
																																										getZip5().then(
																																											(
																																												result
																																											) => {
																																												responseData.Zip5 = result;
																																												getZip().then(
																																													(
																																														result
																																													) => {
																																														responseData.Zip = result;
																																														getHzip().then(
																																															(
																																																result
																																															) => {
																																																responseData.Hzip = result;
																																																getWzip().then(
																																																	(
																																																		result
																																																	) => {
																																																		responseData.Wzip = result;
																																																		getPmijoindate().then(
																																																			(
																																																				result
																																																			) => {
																																																				responseData.Pmijoindate = result;
																																																				getPmijoindate_year().then(
																																																					(
																																																						result
																																																					) => {
																																																						responseData.Pmijoindate_year = result;
																																																						getPmiexpirationdate().then(
																																																							(
																																																								result
																																																							) => {
																																																								responseData.Pmiexpirationdate = result;
																																																								getPmiexpirationdate_year().then(
																																																									(
																																																										result
																																																									) => {
																																																										responseData.Pmiexpirationdate_year = result;
																																																										getPmpdate().then(
																																																											(
																																																												result
																																																											) => {
																																																												responseData.Pmpdate = result;
																																																												getPmpdate_year().then(
																																																													(
																																																														result
																																																													) => {
																																																														responseData.Pmpdate_year = result;
																																																														getChaptercount().then(
																																																															(
																																																																result
																																																															) => {
																																																																responseData.Chaptercount = result;
																																																																getSigscount().then(
																																																																	(
																																																																		result
																																																																	) => {
																																																																		responseData.Sigscount = result;
																																																																		getIndustrycodes().then(
																																																																			(
																																																																				result
																																																																			) => {
																																																																				responseData.Industrycodes = result;
																																																																				getOccupationcodes().then(
																																																																					(
																																																																						result
																																																																					) => {
																																																																						responseData.Occupationcodes = result;
																																																																						priorReportData = responseData;
																																																																						priorReportDate = new Date();
																																																																						resolve(
																																																																							responseData
																																																																						);
																																																																					}
																																																																				);
																																																																			}
																																																																		);
																																																																	}
																																																																);
																																																															}
																																																														);
																																																													}
																																																												);
																																																											}
																																																										);
																																																									}
																																																								);
																																																							}
																																																						);
																																																					}
																																																				);
																																																			}
																																																		);
																																																	}
																																																);
																																															}
																																														);
																																													}
																																												);
																																											}
																																										);
																																									}
																																								);
																																							}
																																						);
																																					}
																																				);
																																			}
																																		);
																																	}
																																);
																															}
																														);
																													}
																												);
																											}
																										);
																									}
																								);
																							}
																						);
																					}
																				);
																			}
																		);
																	}
																);
															}
														);
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
	});
};
