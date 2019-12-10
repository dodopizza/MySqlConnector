using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using MySql.Data.MySqlClient;

namespace MySqlConnector.Core
{
	internal static class DiagnosticListenerExtensions
	{
		public const string DiagnosticListenerName = "MySqlClientDiagnosticListener";
		private const string SqlClientPrefix = "MySql.Data.MySqlClient.";
		private const string SqlClientExecuteScalarName = "ExecuteScalar";
		private const string SqlClientOpenConnectionName = "Open";
		public const string SqlBeforeExecuteCommand = SqlClientPrefix + nameof(WriteCommandBefore);
		public const string SqlAfterExecuteCommand = SqlClientPrefix + nameof(WriteCommandAfter);
		public const string SqlErrorExecuteCommand = SqlClientPrefix + nameof(WriteCommandError);

		public const string SqlBeforeOpenConnection = SqlClientPrefix + nameof(WriteConnectionOpenBefore);
		public const string SqlAfterOpenConnection = SqlClientPrefix + nameof(WriteConnectionOpenAfter);
		public const string SqlErrorOpenConnection = SqlClientPrefix + nameof(WriteConnectionOpenError);

		private static readonly object? EmtptyStatistics = null;
		public static Guid WriteCommandBefore(this DiagnosticListener @this, IReadOnlyList<IMySqlCommand> sqlCommands, string operation = SqlClientExecuteScalarName)
		{
			if (@this.IsEnabled(SqlBeforeExecuteCommand))
			{
				var operationId = Guid.NewGuid();
				foreach (var command in sqlCommands)
				{
					@this.Write(
						SqlBeforeExecuteCommand,
						new
						{
							OperationId = operationId,
							Operation = operation,
							ConnectionId = command.Connection?.GetConnectionId(),
							Command = command
						});
				}


				return operationId;
			}

			return Guid.Empty;
		}

		public static void WriteCommandAfter(this DiagnosticListener @this, Guid operationId, IReadOnlyList<IMySqlCommand> sqlCommands, string operation = SqlClientExecuteScalarName)
        {
            if (@this.IsEnabled(SqlAfterExecuteCommand))
            {
	            foreach (var command in sqlCommands)
	            {
		            @this.Write(
			            SqlAfterExecuteCommand,
			            new
			            {
				            OperationId = operationId,
				            Operation = operation,
				            ConnectionId = command.Connection?.GetConnectionId(),
				            Command = command,
				            Statistics = EmtptyStatistics,
				            Timestamp = Stopwatch.GetTimestamp()
			            });
	            }
            }
        }

        public static void WriteCommandError(this DiagnosticListener @this, Guid operationId, IReadOnlyList<IMySqlCommand> sqlCommands, Exception ex, string operation = SqlClientExecuteScalarName)
        {
            if (@this.IsEnabled(SqlErrorExecuteCommand))
            {
	            foreach (var command in sqlCommands)
	            {
		            @this.Write(
			            SqlErrorExecuteCommand,
			            new
			            {
				            OperationId = operationId,
				            Operation = operation,
				            ConnectionId = command.Connection?.GetConnectionId(),
				            Command = command,
				            Exception = ex,
				            Timestamp = Stopwatch.GetTimestamp()
			            });
	            }
            }
        }

        public static Guid WriteConnectionOpenBefore(this DiagnosticListener @this, MySqlConnection sqlConnection, string operation = SqlClientOpenConnectionName)
        {
            if (@this.IsEnabled(SqlBeforeOpenConnection))
            {
                Guid operationId = Guid.NewGuid();

                @this.Write(
                    SqlBeforeOpenConnection,
                    new
                    {
                        OperationId = operationId,
                        Operation = operation,
                        Connection = sqlConnection,
                        Timestamp = Stopwatch.GetTimestamp()
                    });

                return operationId;
            }
            else
                return Guid.Empty;
        }

        public static void WriteConnectionOpenAfter(this DiagnosticListener @this, Guid operationId, MySqlConnection sqlConnection, string operation = SqlClientOpenConnectionName)
        {
            if (@this.IsEnabled(SqlAfterOpenConnection))
            {
                @this.Write(
                    SqlAfterOpenConnection,
                    new
                    {
                        OperationId = operationId,
                        Operation = operation,
                        ConnectionId = sqlConnection.GetConnectionId(),
                        Connection = sqlConnection,
                        Statistics = EmtptyStatistics,
                        Timestamp = Stopwatch.GetTimestamp()
                    });
            }
        }

        private static int? GetConnectionId(this MySqlConnection @this) => @this.State == ConnectionState.Open ? (int?) @this.ServerThread : null;

        public static void WriteConnectionOpenError(this DiagnosticListener @this, Guid operationId, MySqlConnection sqlConnection, Exception ex, string operation = SqlClientOpenConnectionName)
        {
            if (@this.IsEnabled(SqlErrorOpenConnection))
            {
                @this.Write(
                    SqlErrorOpenConnection,
                    new
                    {
                        OperationId = operationId,
                        Operation = operation,
                        ConnectionId = sqlConnection.GetConnectionId(),
                        Connection = sqlConnection,
                        Exception = ex,
                        Timestamp = Stopwatch.GetTimestamp()
                    });
            }
        }
	}
}
