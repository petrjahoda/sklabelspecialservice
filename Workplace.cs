using System;
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
        public int PreviousWorkshiftId { get; set; }
        public DateTime LastStateDateTime { get; set; }
        public int DefaultOrder { get; set; }
        public int DefaultProduct { get; set; }
        public int WorkplaceDivisionId { get; set; }
        public int ProductionPortOid { get; set; }
        public int CountPortOid { get; set; }
        public int NokPortOid { get; set; }
        public DateTime OrderStartDate { get; set; }
        public int? OrderUserId { get; set; }
        public StateType ActualStateType { get; set; }
        public int WorkplaceIdleId { get; set; }

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

        private object GetNokCountForWorkplace(ILogger logger) {
            var nokCount = 0;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            var startDate = string.Format("{0:yyyy-MM-dd HH:mm:ss.ffff}", OrderStartDate);
            var actualDateTime = DateTime.Now;
            if (Program.TimezoneIsUtc) {
                actualDateTime = DateTime.UtcNow;
            }

            var endDate = string.Format("{0:yyyy-MM-dd HH:mm:ss.ffff}", actualDateTime);

            try {
                connection.Open();
                var selectQuery =
                    $"Select count(oid) as count from zapsi2.device_input_digital where DT>='{startDate}' and DT<='{endDate}' and DevicePortId={NokPortOid} and zapsi2.device_input_digital.Data=1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    while (reader.Read()) {
                        nokCount = Convert.ToInt32(reader["count"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem getting nok count for workplace: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }


            return nokCount;
        }

//
//        public void CloseIdleForWorkplace(DateTime dateTimeToInsert, ILogger logger) {
//            if (Program.CloseOnlyAutomaticIdles.Equals("1")) {
//                var myDate = string.Format("{0:yyyy-MM-dd HH:mm:ss}", LastStateDateTime);
//
//                var dateToInsert = string.Format("{0:yyyy-MM-dd HH:mm:ss}", dateTimeToInsert);
//                if (LastStateDateTime.CompareTo(dateToInsert) > 0) {
//                    dateToInsert = myDate;
//                }
//                if (Program.DatabaseType.Equals("mysql")) {
//                    var connection = new MySqlConnection(
//                        $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                    try {
//                        connection.Open();
//                        var command = connection.CreateCommand();
//                        command.CommandText =
//                            $"UPDATE `zapsi2`.`terminal_input_idle` t SET t.`DTE` = '{dateToInsert}', t.Interval = TIME_TO_SEC(timediff('{dateToInsert}', DTS)) WHERE t.`DTE` is NULL and DeviceID={DeviceOid} and Note like 'Automatic idle'";
//                        try {
//                            command.ExecuteNonQuery();
//                        } catch (Exception error) {
//                            LogError("[ MAIN ] --ERR-- problem closing idle in database: " + error.Message + command.CommandText, logger);
//                        } finally {
//                            command.Dispose();
//                        }
//
//                        connection.Close();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                    } finally {
//                        connection.Dispose();
//                    }
//                } else if (Program.DatabaseType.Equals("sqlserver")) {
//                    var connection = new SqlConnection
//                        {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                    try {
//                        connection.Open();
//                        var command = connection.CreateCommand();
//
//
//                        command.CommandText =
//                            $"UPDATE [dbo].[terminal_input_idle] SET [DTE] = '{dateToInsert}', [Interval] = (datediff(second, DTS, '{dateToInsert}')) WHERE [DTE] is NULL and DeviceID={DeviceOid} and Note like 'Automatic idle'";
//                        try {
//                            command.ExecuteNonQuery();
//                        } catch (Exception error) {
//                            LogError("[ MAIN ] --ERR-- problem closing idle in database: " + error.Message + command.CommandText, logger);
//                        } finally {
//                            command.Dispose();
//                        }
//
//                        connection.Close();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                    } finally {
//                        connection.Dispose();
//                    }
//                }
//            } else {
//                var myDate = string.Format("{0:yyyy-MM-dd HH:mm:ss}", LastStateDateTime);
//                if (Program.DatabaseType.Equals("mysql")) {
//                    var connection = new MySqlConnection(
//                        $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                    try {
//                        connection.Open();
//                        var command = connection.CreateCommand();
//
//
//                        command.CommandText =
//                            $"UPDATE `zapsi2`.`terminal_input_idle` t SET t.`DTE` = '{myDate}', t.Interval = TIME_TO_SEC(timediff('{myDate}', DTS)) WHERE t.`DTE` is NULL and DeviceID={DeviceOid}";
//                        try {
//                            command.ExecuteNonQuery();
//                        } catch (Exception error) {
//                            LogError("[ MAIN ] --ERR-- problem closing idle in database: " + error.Message + command.CommandText, logger);
//                        } finally {
//                            command.Dispose();
//                        }
//
//                        connection.Close();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                    } finally {
//                        connection.Dispose();
//                    }
//                } else if (Program.DatabaseType.Equals("sqlserver")) {
//                    var connection = new SqlConnection
//                        {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                    try {
//                        connection.Open();
//                        var command = connection.CreateCommand();
//
//
//                        command.CommandText =
//                            $"UPDATE [dbo].[terminal_input_idle] SET [DTE] = '{myDate}', [Interval] = (datediff(second, DTS, '{myDate}')) WHERE [DTE] is NULL and DeviceID={DeviceOid}";
//                        try {
//                            command.ExecuteNonQuery();
//                        } catch (Exception error) {
//                            LogError("[ MAIN ] --ERR-- problem closing idle in database: " + error.Message + command.CommandText, logger);
//                        } finally {
//                            command.Dispose();
//                        }
//
//                        connection.Close();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                    } finally {
//                        connection.Dispose();
//                    }
//                }
//            }
//        }
//
//        public void CreateIdleForWorkplace(ILogger logger, bool workplaceHasActiveOrder, DateTime dateTimeToInsert) {
//            var myDate = string.Format("{0:yyyy-MM-dd HH:mm:ss}", dateTimeToInsert);
//            var idleOidToInsert = 2;
//            var userIdToInsert = "";
//            if (workplaceHasActiveOrder) {
//                idleOidToInsert = 1;
//                userIdToInsert = OrderUserId.ToString();
//            }
//            if (userIdToInsert.Length == 0) {
//                userIdToInsert = DownloadFromLoginTable(logger);
//            }
//            if (userIdToInsert.Equals("0") || userIdToInsert.Length == 0) {
//                userIdToInsert = "NULL";
//            }
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    var command = connection.CreateCommand();
//                    command.CommandText =
//                        $"INSERT INTO `zapsi2`.`terminal_input_idle` (`DTS`, `DTE`, `IdleID`, `UserID`, `Interval`, `DeviceID`, `Note`) VALUES ('{myDate}', NULL , {idleOidToInsert}, {userIdToInsert}, 0, {DeviceOid}, 'Automatic idle')";
//
//                    try {
//                        command.ExecuteNonQuery();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- problem inserting idle into database: " + error.Message + command.CommandText, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//                    var command = connection.CreateCommand();
//
//                    command.CommandText =
//                        $"INSERT INTO [dbo].[terminal_input_idle] ([DTS], [DTE], [IdleID], [UserID], [Interval], [DeviceID], [Note]) VALUES ('{myDate}', NULL , {idleOidToInsert}, {userIdToInsert}, 0, {DeviceOid}, 'Automatic idle')";
//                    try {
//                        command.ExecuteNonQuery();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- problem inserting idle into database: " + error.Message + command.CommandText, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//        }
//
//        private string DownloadFromLoginTable(ILogger logger) {
//            var userIdFromLoginTable = "0";
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from zapsi2.terminal_input_login where DTE is NULL and DeviceID={DeviceOid}";
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            userIdFromLoginTable = Convert.ToString(reader["UserID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active order: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from dbo.terminal_input_login where DTE is null and DeviceID={DeviceOid}";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            userIdFromLoginTable = Convert.ToString(reader["UserID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active order: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//            return userIdFromLoginTable;
//        }
//
        public void CloseOrderForWorkplace(DateTime closingDateForOrder, ILogger logger) {
            var myDate = string.Format("{0:yyyy-MM-dd HH:mm:ss}", LastStateDateTime);
            var dateToInsert = string.Format("{0:yyyy-MM-dd HH:mm:ss}", closingDateForOrder);
            if (LastStateDateTime.CompareTo(closingDateForOrder) > 0) {
                dateToInsert = myDate;
            }

            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            var count = GetCountForWorkplace(logger);
            var nokCount = GetNokCountForWorkplace(logger);
            var averageCycle = GetAverageCycleForWorkplace(count);
            var averageCycleToInsert = averageCycle.ToString(CultureInfo.InvariantCulture).Replace(",", ".");
            try {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText =
                    $"UPDATE `zapsi2`.`terminal_input_order` t SET t.`DTE` = '{dateToInsert}', t.Interval = TIME_TO_SEC(timediff('{dateToInsert}', DTS)), t.`Count`={count}, t.Fail={nokCount}, t.averageCycle={averageCycleToInsert} WHERE t.`DTE` is NULL and DeviceID={DeviceOid};" +
                    $"UPDATE zapsi2.terminal_input_login t set t.DTE = '{dateToInsert}', t.Interval = TIME_TO_SEC(timediff('{dateToInsert}', DTS)) where t.DTE is null and t.DeviceId={DeviceOid};";
                try {
                    command.ExecuteNonQuery();
                    LogInfo("[ " + Name + " ] --INF-- ... Order closed", logger);
                } catch (Exception error) {
                    LogError("[ MAIN ] --ERR-- problem closing order in database: " + error.Message + "\n" + command.CommandText, logger);
                } finally {
                    command.Dispose();
                }

                OrderUserId = 0;
                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }
        }

        private int GetCountForWorkplace(ILogger logger) {
            var count = 0;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            var startDate = string.Format("{0:yyyy-MM-dd HH:mm:ss.ffff}", OrderStartDate);
            var actualDateTime = DateTime.Now;
            if (Program.TimezoneIsUtc) {
                actualDateTime = DateTime.UtcNow;
            }

            var endDate = string.Format("{0:yyyy-MM-dd HH:mm:ss.ffff}", actualDateTime);

            try {
                connection.Open();
                var selectQuery =
                    $"Select count(oid) as count from zapsi2.device_input_digital where DT>='{startDate}' and DT<='{endDate}' and DevicePortId={CountPortOid} and zapsi2.device_input_digital.Data=1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    while (reader.Read()) {
                        count = Convert.ToInt32(reader["count"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem getting count for workplace: " + error.Message + selectQuery, logger);
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

        private double GetAverageCycleForWorkplace(int count) {
            var averageCycle = 0.0;
            if (count != 0) {
                var difference = DateTime.Now.Subtract(OrderStartDate).TotalSeconds;
                averageCycle = difference / count;
                if (averageCycle < 0) {
                    averageCycle = 0;
                }
            }

            return averageCycle;
        }

//
//        public void CreateOrderForWorkplace(ILogger logger) {
//            var workplaceModeId = GetWorkplaceModeId(logger);
//            var myDate = string.Format("{0:yyyy-MM-dd HH:mm:ss}", DateTime.Now);
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    var command = connection.CreateCommand();
//                    command.CommandText =
//                        $"INSERT INTO `zapsi2`.`terminal_input_order` (`DTS`, `DTE`, `OrderID`, `UserID`, `DeviceID`, `Interval`, `Count`, `Fail`, `AverageCycle`, `WorkerCount`, `WorkplaceModeID`, `Note`, `WorkshiftID`) VALUES ('{myDate}', NULL, {DefaultOrder}, NULL, {DeviceOid}, 0, DEFAULT, DEFAULT, DEFAULT, DEFAULT, {workplaceModeId}, 'NULL', {ActualWorkshiftId})";
//                    try {
//                        command.ExecuteNonQuery();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- problem inserting terminal input order into database: " + error.Message + command.CommandText, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//                    var command = connection.CreateCommand();
//                    command.CommandText =
//                        $"INSERT INTO [dbo].[terminal_input_order] ([DTS], [DTE], [OrderID], [UserID], [DeviceID], [Interval], [Count], [Fail], [AverageCycle], [WorkerCount], [WorkplaceModeID], [Note], [WorkshiftID]) VALUES ('{myDate}', NULL, {DefaultOrder}, 1, {DeviceOid}, 0, DEFAULT, DEFAULT, DEFAULT, DEFAULT, {workplaceModeId}, 'NULL', {ActualWorkshiftId})";
//                    try {
//                        command.ExecuteNonQuery();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- problem inserting terminal input order into database: " + error.Message + command.CommandText, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//        }
//
//        private int GetWorkplaceModeId(ILogger logger) {
//            var workplaceModeId = 1;
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from zapsi2.workplace_mode where WorkplaceId={Oid}";
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            workplaceModeId = Convert.ToInt32(reader["OID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active order: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from dbo.workplace_mode where WorkplaceId={Oid}";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            workplaceModeId = Convert.ToInt32(reader["OID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active order: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//
//            return workplaceModeId;
//        }
//
//        public bool CheckIfWorkplaceHasActivedIdle(ILogger logger) {
//            var workplaceHasActiveIdle = false;
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from zapsi2.terminal_input_idle where DTE is null and  DeviceID={DeviceOid}";
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            WorkplaceIdleId = Convert.ToInt32(reader["IdleID"]);
//                            workplaceHasActiveIdle = true;
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active idle: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from dbo.[terminal_input_idle] where DTE is NULL and  DeviceID={DeviceOid}";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            workplaceHasActiveIdle = true;
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active idle: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//
//            return workplaceHasActiveIdle;
//        }
//
//        public bool CheckIfWorkplaceHasActiveOrder(ILogger logger) {
//            var workplaceHasActiveOrder = false;
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from zapsi2.terminal_input_order where DTE is NULL and DeviceID={DeviceOid}";
//                    LogInfo("[ " + Name + " ] --INF-- Query: " + selectQuery, logger);
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            workplaceHasActiveOrder = true;
//                            OrderStartDate = Convert.ToDateTime(reader["DTS"]);
//                            try {
//                                OrderUserId = Convert.ToInt32(reader["UserID"]);
//                            } catch (Exception error) {
//                                OrderUserId = null;
//                                LogInfo("[ " + Name + " ] --INF-- Open order has no user", logger);
//                            }
//                        } else {
//                            OrderUserId = null;
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active order: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from dbo.[terminal_input_order] where DTE is NULL and DeviceID={DeviceOid}";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            workplaceHasActiveOrder = true;
//                            OrderStartDate = Convert.ToDateTime(reader["DTS"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active order: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//            LogInfo("[ " + Name + " ] --INF-- Workplace has open order: " + workplaceHasActiveOrder, logger);
//
//            return workplaceHasActiveOrder;
//        }
//
        public void UpdateActualStateForWorkplace(ILogger logger) {
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
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

        public int GetActualWorkShiftIdFor(ILogger logger) {
            PreviousWorkshiftId = ActualWorkshiftId;

            var actualWorkShiftId = 0;
            if (Program.DatabaseType.Equals("mysql")) {
                var connection = new MySqlConnection(
                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
                var actualTime = DateTime.Now;
                if (Program.TimezoneIsUtc) {
                    actualTime = DateTime.UtcNow;
                }

                try {
                    connection.Open();
                    var selectQuery = $"SELECT * from zapsi2.workshift where Active=1 and WorkplaceDivisionID is null or WorkplaceDivisionID ={WorkplaceDivisionId}";
                    var command = new MySqlCommand(selectQuery, connection);
                    try {
                        var reader = command.ExecuteReader();
                        while (reader.Read()) {
                            var oid = Convert.ToInt32(reader["OID"]);
                            var shiftStartsAt = reader["WorkshiftStart"].ToString();
                            var shiftDuration = Convert.ToInt32(reader["WorkshiftLenght"]);
                            var shiftStartAtDateTime = DateTime.ParseExact(shiftStartsAt, "HH:mm:ss", System.Globalization.CultureInfo.CurrentCulture);
                            if (Program.TimezoneIsUtc) {
                                shiftStartAtDateTime = DateTime.ParseExact(shiftStartsAt, "HH:mm:ss", System.Globalization.CultureInfo.CurrentCulture)
                                    .ToUniversalTime();
                            }

                            var shiftEndsAtDateTime = shiftStartAtDateTime.AddMinutes(shiftDuration);
                            if (actualTime.Ticks < shiftStartAtDateTime.Ticks) {
                                actualTime = actualTime.AddDays(1);
                            }

                            if (actualTime.Ticks >= shiftStartAtDateTime.Ticks && actualTime.Ticks < shiftEndsAtDateTime.Ticks) {
                                actualWorkShiftId = oid;
                                LogInfo("[ " + Name + " ] --INF-- Actual workshift id: " + actualWorkShiftId, logger);
                            }
                        }

                        reader.Close();
                        reader.Dispose();
                    } catch (Exception error) {
                        LogError("[ " + Name + " ] --ERR-- Problem getting actual workshift: " + error.Message + selectQuery, logger);
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

            CheckForFirstRunWhenPreviouslyWasNoShift(actualWorkShiftId);
            return actualWorkShiftId;
        }

        private void CheckForFirstRunWhenPreviouslyWasNoShift(int actualWorkShiftId) {
            if (PreviousWorkshiftId == 0) {
                PreviousWorkshiftId = actualWorkShiftId;
            }
        }

//
//        public void AddCountPort(ILogger logger) {
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//
//                    var selectQuery = $"select * from zapsi2.workplace_port where WorkplaceID = {Oid} and Type in ('cycle','running') order by Type asc limit 1;";
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            CountPortOid = Convert.ToInt32(reader["DevicePortID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem reading from database: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//
//                    var selectQuery = $"select top 1 * from dbo.workplace_port where WorkplaceID = {Oid} and Type in ('cycle','running') order by Type asc;";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            CountPortOid = Convert.ToInt32(reader["DevicePortID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem reading from database: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//        }
//
//        public void AddProductionPort(ILogger logger) {
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//
//                    var selectQuery = $"select * from zapsi2.workplace_port where WorkplaceID = {Oid} and Type in ('cycle','running') order by Type desc limit 1;";
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            ProductionPortOid = Convert.ToInt32(reader["DevicePortID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem adding production port: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//
//                    var selectQuery = $"select top 1 * from dbo.[workplace_port] where WorkplaceID = {Oid} and Type in ('cycle','running') order by Type desc;";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            ProductionPortOid = Convert.ToInt32(reader["DevicePortID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem adding production port: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//        }
//
//        public void CreateDefaultOrder(ILogger logger) {
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    var command = connection.CreateCommand();
//
//                    command.CommandText =
//                        $"INSERT INTO `zapsi2`.`order` (`Name`, `Barcode`, `ProductID`, `OrderStatusID`, `CountRequested`, `WorkplaceID`) VALUES ('Internal', '1', {DefaultProduct}, 1, 0, {Oid})";
//                    try {
//                        command.ExecuteNonQuery();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- problem inserting default order into database: " + error.Message + command.CommandText, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//                    var command = connection.CreateCommand();
//
//                    command.CommandText =
//                        $"INSERT INTO [dbo].[order] ([Name], [Barcode], [ProductID], [OrderStatusID], [CountRequested], [WorkplaceID]) VALUES ('Internal', '1', {DefaultProduct}, 1, 0, {Oid})";
//                    try {
//                        command.ExecuteNonQuery();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- problem inserting default order into database: " + error.Message + command.CommandText, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//        }
//
//        public bool CheckForDefaultOrder(ILogger logger) {
//            var defaultOrderIsInDatabase = false;
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    const string selectQuery = @"SELECT * from zapsi2.order where Name like 'Internal' limit 1";
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            DefaultOrder = Convert.ToInt32(reader["OID"]);
//                            defaultOrderIsInDatabase = true;
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- Problem checking default order: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//                    const string selectQuery = @"SELECT TOP 1 * from dbo.[order] where Name like 'Internal';";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            DefaultOrder = Convert.ToInt32(reader["OID"]);
//                            defaultOrderIsInDatabase = true;
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- Problem checking default order: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//
//            return defaultOrderIsInDatabase;
//        }
//
//
//        public bool CheckForDefaultProduct(ILogger logger) {
//            var defaultProductIsInDatabase = false;
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    const string selectQuery = @"SELECT * from zapsi2.product where Name like 'Internal' limit 1";
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            DefaultProduct = Convert.ToInt32(reader["OID"]);
//                            defaultProductIsInDatabase = true;
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- Problem checking default product: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//                try {
//                    connection.Open();
//                    const string selectQuery = @"SELECT TOP 1 * from dbo.[product] where Name like 'Internal'";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            DefaultProduct = Convert.ToInt32(reader["OID"]);
//                            defaultProductIsInDatabase = true;
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- Problem checking default product: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//
//            return defaultProductIsInDatabase;
//        }
//
//        public void CreateDefaultProduct(ILogger logger) {
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    var command = connection.CreateCommand();
//
//                    command.CommandText =
//                        ("INSERT INTO `zapsi2`.`product` (`Name`, `Barcode`, `Cycle`, `IdleFromTime`, `ProductStatusID`, `Deleted`, `ProductGroupID`) VALUES ('Internal', '1', 1, 10, 1, 0, NULL)"
//                        );
//                    try {
//                        command.ExecuteNonQuery();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- problem inserting default product into database: " + error.Message + command.CommandText, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ MAIN ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//                try {
//                    connection.Open();
//                    var command = connection.CreateCommand();
//
//                    command.CommandText =
//                        $"INSERT INTO [dbo].[product] ([Name], [Barcode], [Cycle], [IdleFromTime], [ProductStatusID], [Deleted], [ProductGroupID]) VALUES ('Internal', '1', 1, 10, 1, 0, NULL)";
//                    try {
//                        command.ExecuteNonQuery();
//                    } catch (Exception error) {
//                        LogError("[ MAIN ] --ERR-- problem inserting default product into database: " + error.Message + command.CommandText, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ MAIN ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//        }
//
//        public bool CheckForOneFromLastOrderDte(ILogger logger) {
//            var productionIsOne = false;
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    var selectQuery =
//                        $"Select * from zapsi2.device_input_digital where DevicePortID={ProductionPortOid} and DT > (select DTE from terminal_input_order where DeviceID = {DeviceOid} order by DTS desc limit 1) order by DT desc limit 2";
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            var data = Convert.ToInt32(reader["Data"]);
//                            var lastDataDateTime = Convert.ToDateTime(reader["DT"]);
//                            if (Program.TimezoneIsUtc) {
//                                if (data == 1 && (DateTime.UtcNow - lastDataDateTime).TotalSeconds < 10) {
//                                    productionIsOne = true;
//                                }
//                            } else {
//                                if (data == 1 && (DateTime.Now - lastDataDateTime).TotalSeconds < 10) {
//                                    productionIsOne = true;
//                                }
//                            }
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem getting count for workplace: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//                    var selectQuery =
//                        $"Select top 2 from dbo.device_input_digital where DevicePortID={ProductionPortOid} and DT > (select top1 DTE from terminal_input_order where DeviceID = {DeviceOid} order by DTS desc) order by DT desc";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            var data = Convert.ToInt32(reader["Data"]);
//                            var lastDataDateTime = Convert.ToDateTime(reader["DT"]);
//                            if (data == 1 && (DateTime.Now - lastDataDateTime).TotalSeconds < 10) {
//                                productionIsOne = true;
//                            }
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem getting count for workplace: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//
//            return productionIsOne;
//        }
//
//        public void AddFailPort(ILogger logger) {
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//
//                    var selectQuery = $"select * from zapsi2.workplace_port where WorkplaceID = {Oid} and Type in ('fail') order by Type asc limit 1;";
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            NokPortOid = Convert.ToInt32(reader["DevicePortID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem reading from database: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//
//                    var selectQuery = $"select top 1 * from dbo.workplace_port where WorkplaceID = {Oid} and Type in ('fail') order by Type asc;";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        while (reader.Read()) {
//                            NokPortOid = Convert.ToInt32(reader["DevicePortID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem reading from database: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//        }
//
//
//        public void CloseAndStartOrderForWorkplaceAt(DateTime workshiftStartsAt, ILogger logger) {
//            var workplaceModeId = GetWorkplaceModeId(logger);
//            var orderId = GetOrderId(logger);
//            var cavity = GetCavityForOrder(logger);
//            var userId = OrderUserId;
//            var anyOrderisOpen = orderId != 0;
//            CheckIfWorkplaceHasActiveOrder(logger);
//            if (anyOrderisOpen) {
//                var closingOnPowerOff = false;
//                CloseOrderForWorkplace(workshiftStartsAt, closingOnPowerOff, logger);
//                CreateOrderForWorkplace(workshiftStartsAt, orderId, userId, workplaceModeId, cavity, logger);
//            }
//        }
//
//        private int GetCavityForOrder(ILogger logger) {
//            var cavity = 1;
//            var connection = new MySqlConnection(
//                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//            try {
//                connection.Open();
//                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where DeviceID={DeviceOid} and DTE is null";
//                var command = new MySqlCommand(selectQuery, connection);
//                try {
//                    var reader = command.ExecuteReader();
//                    if (reader.Read()) {
//                        cavity = Convert.ToInt32(reader["Cavity"]);
//                    }
//
//                    reader.Close();
//                    reader.Dispose();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem checking active order: " + error.Message + selectQuery, logger);
//                } finally {
//                    command.Dispose();
//                }
//
//                connection.Close();
//            } catch (Exception error) {
//                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//            } finally {
//                connection.Dispose();
//            }
//
//
//            return cavity;
//        }
//
        public void CreateOrderForWorkplace(DateTime date, int orderId, int userId, int workplaceModeId, int cavity, ILogger logger) {
            string userToInsert = "NULL";
            if (userId > 0) {
                userToInsert = userId.ToString();
            }

            int actualWorkshiftId = GetActualWorkShiftIdFor(logger);
            var dateToInsert = $"{date:yyyy-MM-dd HH:mm:ss}";
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText =
                    $"INSERT INTO `zapsi2`.`terminal_input_order` (`DTS`, `DTE`, `OrderID`, `UserID`, `DeviceID`, `Interval`, `Count`, `Fail`, `AverageCycle`, `WorkerCount`, `WorkplaceModeID`, `Note`, `WorkshiftID`, `Cavity`)" +
                    $" VALUES ('{dateToInsert}', NULL, {orderId}, {userToInsert}, {DeviceOid}, 0, DEFAULT, DEFAULT, DEFAULT, DEFAULT, {workplaceModeId}, 'NULL', {actualWorkshiftId}, {cavity})";
                try {
                    command.ExecuteNonQuery();
                    LogInfo("[ " + Name + " ] --INF-- ... Order created", logger);
                } catch (Exception error) {
                    LogError("[ MAIN ] --ERR-- problem inserting terminal input order into database: " + error.Message + command.CommandText, logger);
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
//
//        private int GetOrderId(ILogger logger) {
//            var actualOrderId = 0;
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from zapsi2.terminal_input_order where DeviceID={DeviceOid} and DTE is null";
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            actualOrderId = Convert.ToInt32(reader["OrderID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active order: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from dbo.terminal_input_order where DeviceID={DeviceOid} and DTE is null";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            actualOrderId = Convert.ToInt32(reader["OrderID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active order: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//
//            return actualOrderId;
//        }
//
//        public void CloseAndStartIdleForWorkplaceAt(DateTime workshiftStartsAt, ILogger logger) {
//            var idleId = GetIdleId(logger);
//            var anyIdleIsOpen = (idleId != 0);
//            if (anyIdleIsOpen) {
//                var workplaceHasActiveOrder = CheckIfWorkplaceHasActiveOrder(logger);
//                CloseIdleForWorkplace(workshiftStartsAt, logger);
//                CreateIdleForWorkplace(logger, workplaceHasActiveOrder, workshiftStartsAt);
//            }
//        }
//
//        private int GetIdleId(ILogger logger) {
//            var actualIdleId = 0;
//            if (Program.DatabaseType.Equals("mysql")) {
//                var connection = new MySqlConnection(
//                    $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from zapsi2.terminal_input_idle where DeviceID={DeviceOid} and DTE is null";
//                    var command = new MySqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            actualIdleId = Convert.ToInt32(reader["IdleID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active idle: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            } else if (Program.DatabaseType.Equals("sqlserver")) {
//                var connection = new SqlConnection
//                    {ConnectionString = $"Data Source={Program.IpAddress}; Initial Catalog={Program.Database}; User id={Program.Login}; Password={Program.Password};"};
//
//                try {
//                    connection.Open();
//                    var selectQuery = $"SELECT * from dbo.terminal_input_idle where DeviceID={DeviceOid} and DTE is null";
//                    var command = new SqlCommand(selectQuery, connection);
//                    try {
//                        var reader = command.ExecuteReader();
//                        if (reader.Read()) {
//                            actualIdleId = Convert.ToInt32(reader["IdleID"]);
//                        }
//
//                        reader.Close();
//                        reader.Dispose();
//                    } catch (Exception error) {
//                        LogError("[ " + Name + " ] --ERR-- Problem checking active idle: " + error.Message + selectQuery, logger);
//                    } finally {
//                        command.Dispose();
//                    }
//
//                    connection.Close();
//                } catch (Exception error) {
//                    LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
//                } finally {
//                    connection.Dispose();
//                }
//            }
//
//            return actualIdleId;
//        }

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
                Console.WriteLine(selectQuery);
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

            var userIdToreturn = 0;
            try {
                userIdToreturn = Convert.ToInt32(userId);
            } catch (Exception e) {
                LogError("[ " + Name + " ] --ERR-- Problem converting userid: " + userId, logger);
            }

            return userIdToreturn;
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


        public int GetIdleTimeFor(int workplaceModeId, ILogger logger) {
            var idleTime = 0;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.workplace_mode where Workplaceid={Oid} and WorkplaceModeTypeId={workplaceModeId} limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        idleTime = Convert.ToInt32(reader["IdleFromTime"]);
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

            return idleTime;
        }

        public void UpdateIdleTimeForWorkplace(int idleTime, ILogger logger) {
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var command = connection.CreateCommand();

                command.CommandText =
                    $"UPDATE `zapsi2`.`workplace` t SET t.IdleFromTime={idleTime} where OID={Oid}";

                try {
                    command.ExecuteNonQuery();
                    LogInfo("[ " + Name + " ] --INF-- ... Idle time updated", logger);
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- problem updating idle time for workplace: " + error.Message + command.CommandText, logger);
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

        public int GetPowerOffClosedOrderId(ILogger logger) {
            var orderClosedAutomaticallyWhenPowerOff = 0;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where DeviceId={DeviceOid} and Note like 'Poweroff closed' limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        orderClosedAutomaticallyWhenPowerOff = Convert.ToInt32(reader["OID"]);
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

            return orderClosedAutomaticallyWhenPowerOff;
        }

        public void UpdateNoteForTerminalInputOrder(string note, int powerOffClosedOrderId, ILogger logger) {
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var command = connection.CreateCommand();

                command.CommandText = $"UPDATE `zapsi2`.`terminal_input_order` t SET t.`Note`='{note}' where t.`OID`={powerOffClosedOrderId}";

                try {
                    command.ExecuteNonQuery();
                    LogInfo("[ " + Name + " ] --INF-- ... Terminal input order updated with string Saved to K2", logger);
                } catch (Exception error) {
                    LogError($"[ {Name} ] --ERR-- problem updating terminal input order with note {note}: {error.Message}{command.CommandText}", logger);
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

        public int GetAnyOpenTerminalInputOrderFor(ILogger logger) {
            var openTerminalInputOrderId = 0;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where DeviceId={DeviceOid} and DTE is null  limit 1";
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

        public int GetAnyOpenIdleIdForOrderNotSavedToK2(ILogger logger) {
            var openTerminalInputIdleId = 0;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_idle where DeviceId={DeviceOid} and DTE is null and Note is null limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        openTerminalInputIdleId = Convert.ToInt32(reader["OID"]);
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

            return openTerminalInputIdleId;
        }

        public void UpdateNoteForIdle(int openIdleId, string note, ILogger logger) {
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var command = connection.CreateCommand();

                command.CommandText = $"UPDATE `zapsi2`.`terminal_input_idle` t SET t.`Note`='{note}' where t.`OID`={openIdleId}";

                try {
                    command.ExecuteNonQuery();
                    LogInfo("[ " + Name + " ] --INF-- ... Terminal input idle updated with string Saved to K2", logger);
                } catch (Exception error) {
                    LogError($"[ {Name} ] --ERR-- problem updating terminal input idle with note {note}: {error.Message}{command.CommandText}", logger);
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

        public int GetWorkplaceModeIdForOpenOrder(int openTerminalInputOrderId, ILogger logger) {
            var workplaceModeId = 0;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where OID={openTerminalInputOrderId}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        workplaceModeId = Convert.ToInt32(reader["WorkplaceModeId"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking workplace mode for open order: " + error.Message + selectQuery, logger);
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

        public int GetWorkplaceModeTypeIdForOpenOrder(int workplaceModeId, ILogger logger) {
            var workplaceModeTypeId = 0;
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.workplace_mode where OID={workplaceModeId}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        workplaceModeTypeId = Convert.ToInt32(reader["WorkplaceModeTypeId"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking workplace mode type id: " + error.Message + selectQuery, logger);
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

        public string GetWorkplaceModeTypeNameFor(int workplaceModeTypeId, ILogger logger) {
            var workplaceModeTypeName = "null";
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.workplace_mode_type where OID={workplaceModeTypeId}";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        workplaceModeTypeName = Convert.ToString(reader["Name"]);
                    }

                    reader.Close();
                    reader.Dispose();
                } catch (Exception error) {
                    LogError("[ " + Name + " ] --ERR-- Problem checking workplace mode type id: " + error.Message + selectQuery, logger);
                } finally {
                    command.Dispose();
                }

                connection.Close();
            } catch (Exception error) {
                LogError("[ " + Name + " ] --ERR-- Problem with database: " + error.Message, logger);
            } finally {
                connection.Dispose();
            }

            return workplaceModeTypeName;
        }

        public void SaveToK2(string codeForK2, int userId, int orderId, ILogger logger) {
            var workplaceCode = GetWorkplaceCode(logger);
            var userLogin = GetUserLoginFor(userId, logger);
            var orderName = GetOrderNameFor(orderId, logger);
            var data = "\\id_stroj{" + workplaceCode + "}\\id_osoby{" + userLogin + "}\\id_zakazky{" + orderName + "}\\id_krok{" + codeForK2 + "}\\id_operace{" + orderName + "}";
            LogInfo($"[ {Name} ] --INF-- Saving {codeForK2} to K2 for workplace.code={workplaceCode}, order.name={orderName} and user.login={userLogin}", logger);
            var connection = new System.Data.SqlClient.SqlConnection {ConnectionString = "Data Source=10.3.1.3; Initial Catalog=K2_SKLABEL; User id=zapsi; Password=DSgEEmPNxCwgTJjsd2uR;"};
            LogInfo($"[ {Name} ] --INF-- Connection string {connection.ConnectionString}", logger);
            LogInfo($"[ {Name} ] --INF-- Printed", logger);

            try {
                connection.Open();
                LogInfo("[ MAIN ] --INF-- connection open", logger);

                var command = connection.CreateCommand();
                command.CommandText =
                    $"INSERT INTO [dbo].[ZAPSI_K2] ([id_zaznamu], [cas], [typ], [data], [zprac], [cas_zprac], [error], [castrigger]) VALUES ('', GETDATE() , '200', '{data}', 0, NULL, NULL, NULL);";
                LogInfo($"[ MAIN ] --INF-- {command.CommandText}", logger);
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

        public bool CheckIfThirtyPiecesAreDone(int idForWorkplaceMode, ILogger logger) {
            var thirtyPiecesAreDone = false;
            var piecesDone = 0;
            // get datetime for open terminal input order
            // get true if more than 30 pieces are done
            var connection = new MySqlConnection(
                $"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where DeviceId={DeviceOid} and WorkplaceModeId={idForWorkplaceMode} and DTE is null  limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        piecesDone = Convert.ToInt32(reader["Count"]);
                    }

                    LogInfo("[ " + Name + " ] --INF-- Pieces done for order: " + piecesDone, logger);

                    if (piecesDone > 30) {
                        thirtyPiecesAreDone = true;
                        LogInfo("[ " + Name + " ] --INF-- Thirty pieces done", logger);
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

            return thirtyPiecesAreDone;
        }

        public void UpdateCountFromAnalog(ILogger logger) {
            var portOid = GetAnalogPortForWorkplace(logger);
            var openTerminalInputOrder = GetOpenOrderForAnalogProcessing(logger);
            LogInfo("[ " + Name + " ] --INF-- Analog port OID: " + portOid, logger);
            var count = GetCountForPort(portOid, logger);
            LogInfo("[ " + Name + " ] --INF-- Count for open order: " + count, logger);
            var orderStartDate = GetOrderDTS(openTerminalInputOrder, logger);
            LogInfo("[ " + Name + " ] --INF-- Order start date: " + orderStartDate, logger);
            var difference = DateTime.Now.Subtract(orderStartDate).TotalSeconds.ToString(CultureInfo.InvariantCulture);
            LogInfo("[ " + Name + " ] --INF-- Interval: " + difference, logger);
            var averageCycle = DateTime.Now.Subtract(orderStartDate).TotalSeconds / count;
            LogInfo("[ " + Name + " ] --INF-- Average cycle: " + averageCycle, logger);
            var averageCycleToInsert = averageCycle.ToString(CultureInfo.InvariantCulture).Replace(",", ".");
            LogInfo("[ " + Name + " ] --INF-- Average cycle for insert: " + averageCycleToInsert, logger);
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var command = connection.CreateCommand();

                command.CommandText =
                    $"UPDATE `zapsi2`.`terminal_input_order` t SET t.Count={count}, t.AverageCycle={averageCycleToInsert}, t.Interval={difference} WHERE t.`DTE` is NULL and DeviceID={DeviceOid}";
                LogInfo("[ " + Name + " ] --INF-- Update query: " + command.CommandText, logger);

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
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where DeviceId={DeviceOid} and and DTE is null limit 1";
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

        private int GetCountForPort(int portOid, ILogger logger) {
            var startDate = $"{OrderStartDate:yyyy-MM-dd HH:mm:ss.ffff}";
            var count = 0;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT count(Data) as Count from zapsi2.device_input_analog where DevicePortId={portOid} and DT>'{startDate}'";
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

        private DateTime GetOrderDTS(int openTerminalInputOrder, ILogger logger) {
            var orderDTS = DateTime.Now;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.terminal_input_order where OID={openTerminalInputOrder} limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        orderDTS = Convert.ToDateTime(reader["DTS"]);
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

            return orderDTS;
        }

        private int GetAnalogPortForWorkplace(ILogger logger) {
            var analogPortOid = 1;
            var connection = new MySqlConnection($"server={Program.IpAddress};port={Program.Port};userid={Program.Login};password={Program.Password};database={Program.Database};");
            try {
                connection.Open();
                var selectQuery = $"SELECT * from zapsi2.workplace_port where WorkplaceID={Oid} and HighValue=100 limit 1";
                var command = new MySqlCommand(selectQuery, connection);
                try {
                    var reader = command.ExecuteReader();
                    if (reader.Read()) {
                        analogPortOid = Convert.ToInt32(reader["DevicePortId"]);
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
    }
}