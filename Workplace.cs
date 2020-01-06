using System;
using System.Data.SqlClient;
using System.Globalization;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using static System.Console;

namespace sklabelspecialservice {
    public enum StateType {
        Idle,
        Running,
        PowerOff
    }

    public class Workplace {
        public int Oid { get; set; }
        public string Name { get; set; }
        public int DeviceOid { get; set; }
        public int ActualWorkshiftId { get; set; }
        private DateTime LastStateDateTime { get; set; }
        public int DefaultOrder { get; set; }
        private int DefaultProduct { get; set; }
        public int WorkplaceDivisionId { get; set; }
        private int ProductionPortOid { get; set; }
        private int CountPortOid { get; set; }
        private DateTime OrderStartDate { get; set; }
        private int? OrderUserId { get; set; }
        public StateType ActualStateType { get; set; }
        private int WorkplaceIdleId { get; set; }

        public Workplace() {
            Oid = Oid;
            Name = Name;
            DeviceOid = DeviceOid;
            ActualWorkshiftId = ActualWorkshiftId;
            LastStateDateTime = LastStateDateTime;
            DefaultOrder = DefaultOrder;
            DefaultProduct = DefaultProduct;
            WorkplaceDivisionId = WorkplaceDivisionId;
            ProductionPortOid = ProductionPortOid;
            CountPortOid = CountPortOid;
            OrderStartDate = OrderStartDate;
            ActualStateType = ActualStateType;
        }

        private static void LogInfo(string text, ILogger logger) {
            var now = DateTime.Now;
            text = now + " " + text;
            if (Program.OsIsLinux) {
                WriteLine(Program.greenColor + text + Program.resetColor);
            } else {
                logger.LogInformation(text);
                ForegroundColor = ConsoleColor.Green;
                WriteLine(text);
                ForegroundColor = ConsoleColor.White;
            }
        }


        private static void LogError(string text, ILogger logger) {
            var now = DateTime.Now;
            text = now + " " + text;
            if (Program.OsIsLinux) {
                WriteLine(Program.redColor + text + Program.resetColor);
            } else {
                logger.LogInformation(text);
                ForegroundColor = ConsoleColor.Red;
                WriteLine(text);
                ForegroundColor = ConsoleColor.White;
            }
        }


        public void UpdateActualStateForWorkplace(ILogger logger) {
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                var stateNumber = 3;
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.workplace_state where DTE is NULL and WorkplaceID={Oid}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        LastStateDateTime = Convert.ToDateTime(reader["DTS"].ToString());
                        stateNumber = Convert.ToInt32(reader["StateID"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem getting actual workplace state: " + error.Message + selectQuery, logger);
                }

                selectQuery = $"SELECT * from zapsi2.state where OID={stateNumber}";
                command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        var stateType = Convert.ToString(reader["Type"]);
                        if (stateType.Equals("running")) {
                            ActualStateType = StateType.Running;
                        } else if (stateType.Equals("idle")) {
                            ActualStateType = StateType.Idle;
                        } else {
                            ActualStateType = StateType.PowerOff;
                        }
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem getting actual workplace state: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }


                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }
        }


        public int GetWorkplaceModeTypeIdFor(string workplaceModeName, ILogger logger) {
            var workplaceModeTypeId = 1;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.workplace_mode_type where Name like '{workplaceModeName}' limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        workplaceModeTypeId = Convert.ToInt32(reader["OID"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking workplace mode type ID: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return workplaceModeTypeId;
        }

        public int GetWorkplaceModeIdFor(int idForWorkplaceModeTypeMyti, ILogger logger) {
            var workplaceModeId = 1;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.workplace_mode where Workplaceid={Oid} and WorkplaceModeTypeId={idForWorkplaceModeTypeMyti} limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        workplaceModeId = Convert.ToInt32(reader["OID"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking workplace mode ID: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return workplaceModeId;
        }

        public int GetOpenTerminalInputOrderFor(int idForWorkplaceMode, ILogger logger) {
            var openTerminalInputOrderId = 0;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where DeviceId={DeviceOid} and WorkplaceModeId={idForWorkplaceMode} and DTE is null  limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        openTerminalInputOrderId = Convert.ToInt32(reader["OID"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking open order for workplace: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return openTerminalInputOrderId;
        }

        public int GetUserIdFor(int openTerminalInputOrder, ILogger logger) {
            string userId = "0";
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where OID={openTerminalInputOrder}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        userId = Convert.ToString(reader["UserID"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking userid for workplace: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            var userIdToReturn = 0;
            try {
                userIdToReturn = Convert.ToInt32(userId);
            } catch (Exception e) {
                LogError("[ " + Name + " ] --ERR-- Problem converting userid: " + userId + " " + e.Message, logger);
            }

            return userIdToReturn;
        }

        public int GetOrderIdFor(int openTerminalInputOrder, ILogger logger) {
            var orderId = 101;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where OID={openTerminalInputOrder}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        orderId = Convert.ToInt32(reader["OrderId"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking orderid for workplace: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return orderId;
        }


        public void SaveToK2(string codeForK2, int userId, int orderId, ILogger logger) {
            var workplaceCode = GetWorkplaceCode(logger);
            var userLogin = GetUserLoginFor(userId, logger);
            var orderName = GetOrderNameFor(orderId, logger);
            var data = "\\id_stroj{" + workplaceCode + "}\\id_osoby{" + userLogin + "}\\id_zakazky{" + orderName + "}\\id_krok{" + codeForK2 + "}\\id_operace{" + orderName + "}";
            LogInfo($"[ {Name} ] --INF-- Saving {codeForK2} to K2 for workplace.code={workplaceCode}, order.name={orderName} and user.login={userLogin}", logger);
            var connection = new SqlConnection {ConnectionString = "Data Source=10.3.1.3; Initial Catalog=K2_SKLABEL; User id=zapsi; Password=DSgEEmPNxCwgTJjsd2uR;"};
            LogInfo($"[ {Name} ] --INF-- Connection string {connection.ConnectionString}", logger);
            try {
                connection.Open();
                LogInfo("[ MAIN ] --INF-- connection open", logger);

                var command = connection.CreateCommand();
                command.CommandText =
                    $"INSERT INTO [dbo].[ZAPSI_K2] ([cas], [typ], [data], [zprac], [cas_zprac], [error], [castrigger]) VALUES (GETDATE() , '200', '{data}', 0, NULL, NULL, NULL);";
                try {
                    command.ExecuteNonQuery();
                } catch (Exception error) {
                    LogError("[ MAIN ] --ERR-- problem inserting new record into K2 database: " + error.Message + command.CommandText, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database K2: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }
        }

        private string GetOrderNameFor(int orderId, ILogger logger) {
            var orderName = "null";


            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.order where OID={orderId}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        orderName = Convert.ToString(reader["Barcode"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem getting order name: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return orderName;
        }

        private string GetUserLoginFor(int userId, ILogger logger) {
            var userLogin = "null";
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.user where OID={userId}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        userLogin = Convert.ToString(reader["Login"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem getting user login: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return userLogin;
        }

        private string GetWorkplaceCode(ILogger logger) {
            var workplaceCode = "null";
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.workplace where OID={Oid}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        workplaceCode = Convert.ToString(reader["Code"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem getting workplace code: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return workplaceCode;
        }

        public void UpdateCountFromAnalog(ILogger logger) {
            var portOid = GetAnalogPortForWorkplace(logger);
            var openTerminalInputOrder = GetOpenOrderForAnalogProcessing(logger);
            LogInfo("[ " + Name + " ] --INF-- Analog port OID: " + portOid, logger);
            var orderStartDate = GetOrderDts(openTerminalInputOrder, logger);
            LogInfo("[ " + Name + " ] --INF-- Order start date: " + orderStartDate, logger);
            var count = GetCountForPort(orderStartDate, portOid, logger);
            LogInfo("[ " + Name + " ] --INF-- Count for open order: " + count, logger);
            var difference = DateTime.Now.Subtract(orderStartDate).TotalSeconds.ToString(CultureInfo.InvariantCulture);
            LogInfo("[ " + Name + " ] --INF-- Interval: " + difference, logger);
            var averageCycle = DateTime.Now.Subtract(orderStartDate).TotalSeconds / count;
            if (double.IsInfinity(averageCycle)) {
                averageCycle = 0.0;
            }

            LogInfo("[ " + Name + " ] --INF-- Average cycle: " + averageCycle, logger);
            var averageCycleToInsert = averageCycle.ToString(CultureInfo.InvariantCulture).Replace(",", ".");
            LogInfo("[ " + Name + " ] --INF-- Average cycle for insert: " + averageCycleToInsert, logger);
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var command = connection.CreateCommand();

                command.CommandText =
                    $"UPDATE `zapsi2`.`terminal_input_order` t SET t.Count={count}, t.AverageCycle={averageCycleToInsert}, t.Interval={difference} WHERE t.`DTE` is NULL and DeviceID={DeviceOid}";

                try {
                    command.ExecuteNonQuery();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- problem updating count order: " + error.Message + command.CommandText, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }
        }


        private int GetOpenOrderForAnalogProcessing(ILogger logger) {
            var openTerminalInputOrderId = 0;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where DeviceId={DeviceOid} and DTE is null limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        openTerminalInputOrderId = Convert.ToInt32(reader["OID"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking open order for workplace: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return openTerminalInputOrderId;
        }

        private int GetCountForPort(DateTime orderStartDate, int portOid, ILogger logger) {
            var startDate = $"{orderStartDate:yyyy-MM-dd HH:mm:ss.ffff}";
            var count = 0;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT sum(Data) as Count from zapsi2.device_input_analog where DevicePortId={portOid} and DT>'{startDate}'";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        count = Convert.ToInt32(reader["Count"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking open order for workplace: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return count;
        }

        private DateTime GetOrderDts(int openTerminalInputOrder, ILogger logger) {
            var orderDts = DateTime.Now;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where OID={openTerminalInputOrder} limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        orderDts = Convert.ToDateTime(reader["DTS"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking open order for workplace: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return orderDts;
        }

        private int GetAnalogPortForWorkplace(ILogger logger) {
            var analogPortOid = 1;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();

                var selectQuery = $"select * from zapsi2.device_port where OID in (SELECT DevicePortID from zapsi2.workplace_port where WorkplaceID={Oid}) and PortNumber=120";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        analogPortOid = Convert.ToInt32(reader["OID"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking open order for workplace: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return analogPortOid;
        }

        public void CloseIdleForWorkplace(DateTime dateTimeToInsert, ILogger logger) {
            var dateToInsert = string.Format("{0:yyyy-MM-dd HH:mm:ss}", DateTime.Now);

            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var command = connection.CreateCommand();


                command.CommandText =
                    $"UPDATE `zapsi2`.`terminal_input_idle` t SET t.`DTE` = '{dateToInsert}', t.Interval = TIME_TO_SEC(timediff('{dateToInsert}', DTS)) WHERE t.`DTE` is NULL and DeviceID={DeviceOid}";
                try {
                    command.ExecuteNonQuery();
                } catch (Exception error) {
                    LogError("[ MAIN ] --ERR-- problem closing idle in database: " + error.Message + command.CommandText, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            LogInfo("[ " + Name + " ] --INF-- Terminal_input_idle closed", logger);
        }

        private string DownloadFromLoginTable(ILogger logger) {
            var userIdFromLoginTable = "NULL";
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_login where DTE is NULL and DeviceID={DeviceOid}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        userIdFromLoginTable = Convert.ToString(reader["UserID"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking active order: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return userIdFromLoginTable;
        }

        public void CreateIdleMytiForWorkplace(ILogger logger, bool workplaceHasActiveOrder, DateTime dateTimeToInsert) {
            var myDate = string.Format("{0:yyyy-MM-dd HH:mm:ss}", dateTimeToInsert);
            string userIdToInsert;
            if (workplaceHasActiveOrder) {
                userIdToInsert = DownloadFromLoginTable(logger);
            } else {
                userIdToInsert = "NULL";
            }

            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText =
                    $"INSERT INTO `zapsi2`.`terminal_input_idle` (`DTS`, `DTE`, `IdleID`, `UserID`, `Interval`, `DeviceID`, `Note`) VALUES ('{myDate}', NULL , 11, {userIdToInsert}, 0, {DeviceOid}, 'Automatic idle')";

                try {
                    command.ExecuteNonQuery();
                } catch (Exception error) {
                    LogError("[ MAIN ] --ERR-- problem inserting idle into database: " + error.Message + command.CommandText, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }
        }

        public bool CheckIfWorkplaceHasSpecialIdleOpened(ILogger logger) {
            var workplaceHasSpecialIdleOpened = false;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_idle where DTE is null and DeviceID={DeviceOid} and IdleID in (SELECT OID from zapsi2.idle where IdleTypeID = 101 or OID in (10,11))";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        workplaceHasSpecialIdleOpened = true;
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking active idle: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return workplaceHasSpecialIdleOpened;
        }

        public bool CheckIfWorkplaceHasOpenIdle(ILogger logger) {
            var workplaceHasStandardOpenIdle = false;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_idle where DTE is null and  DeviceID={DeviceOid}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        WorkplaceIdleId = Convert.ToInt32(reader["IdleID"]);
                        workplaceHasStandardOpenIdle = true;
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking active idle: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return workplaceHasStandardOpenIdle;
        }

        public bool CheckIfWorkplaceHasOpenInternalIdle(ILogger logger) {
            var idleId = 0;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_idle where DeviceId={DeviceOid} and DTE is null";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        idleId = Convert.ToInt32(reader["IdleID"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking idle time for workplace mode: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            if (idleId == 1 || idleId == 2) {
                return true;
            }

            return false;
        }

        public bool CheckIfWorkplaceHasNoteMyti(ILogger logger) {
            var idleHasNoteMyti = false;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_idle where DTE is null and DeviceID={DeviceOid} and Note = 'MYTI'";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        idleHasNoteMyti = true;
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking idle for Note Myti: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return idleHasNoteMyti;
        }

        public bool CheckIfWorkplaceIdleIsInternal(ILogger logger) {
            var idleHasNoteMyti = false;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_idle where DTE is null and DeviceID={DeviceOid} and IdleID in (1, 2)";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        idleHasNoteMyti = true;
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking idle for Note Myti: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return idleHasNoteMyti;
        }

        public void CreateIdleInternalForWorkplace(ILogger logger, bool workplaceHasActiveOrder, DateTime dateTimeToInsert) {
            var myDate = $"{dateTimeToInsert:yyyy-MM-dd HH:mm:ss}";
            string userIdToInsert;
            if (workplaceHasActiveOrder) {
                userIdToInsert = DownloadFromLoginTable(logger);
            } else {
                userIdToInsert = "NULL";
            }

            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText =
                    $"INSERT INTO `zapsi2`.`terminal_input_idle` (`DTS`, `DTE`, `IdleID`, `UserID`, `Interval`, `DeviceID`, `Note`) VALUES ('{myDate}', NULL , 1, {userIdToInsert}, 0, {DeviceOid}, 'Automatic idle')";

                try {
                    command.ExecuteNonQuery();
                } catch (Exception error) {
                    LogError("[ MAIN ] --ERR-- problem inserting idle into database: " + error.Message + command.CommandText, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }
        }

        public bool CheckOpenTerminalInputOrder(ILogger logger) {
            var workplaceHasOpenedOrder = false;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where DeviceId={DeviceOid}  and DTE is null limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        workplaceHasOpenedOrder = true;
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking open order for workplace: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return workplaceHasOpenedOrder;
        }

        public bool CheckIfWorkplaceHasNormalIdleOpened(ILogger logger) {
            var normalIdleOpened = false;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_idle where DTE is null and DeviceID={DeviceOid}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        normalIdleOpened = true;
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking active idle: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return normalIdleOpened;
        }



        public bool CheckIfDevicePortIdIsOne(Workplace workplace, ILogger logger) {
            var data = 0;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"select Data from device_input_digital where DevicePortId=(SELECT `DevicePortID` FROM `workplace_port` WHERE `WorkplaceID` = '{workplace.Oid}' AND `Type` LIKE '%running%') order by DT desc limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        data = Convert.ToInt32(reader["Data"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking active idle: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return data == 1;
        }
    }
}