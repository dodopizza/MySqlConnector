using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace MySqlConnector.Core
{
	internal static class DiagnosticListenerExtensions
	{
		public const string DiagnosticListenerName = "MySqlClientDiagnosticListener";
		private const string SqlClientPrefix = "MySql.Data.MySqlClient.";
		private const string SqlClientExecuteScalarName = "ExecuteScalar";
		private const string SqlClientOpenConnectionName = "Open";
		private const string SqlClientCloseConnectionName = "Close";
		private const string SqlClientCommitName = "Commit";
		private const string SqlClientRollbackName = "Rollback";
		private const string SqlBeforeExecuteCommand = SqlClientPrefix + nameof(WriteCommandBefore);
		private const string SqlAfterExecuteCommand = SqlClientPrefix + nameof(WriteCommandAfter);
		private const string SqlErrorExecuteCommand = SqlClientPrefix + nameof(WriteCommandError);

		private const string SqlBeforeOpenConnection = SqlClientPrefix + nameof(WriteConnectionOpenBefore);
		private const string SqlAfterOpenConnection = SqlClientPrefix + nameof(WriteConnectionOpenAfter);
		private const string SqlErrorOpenConnection = SqlClientPrefix + nameof(WriteConnectionOpenError);

		private const string SqlBeforeCloseConnection = SqlClientPrefix + nameof(WriteConnectionCloseBefore);
		private const string SqlAfterCloseConnection = SqlClientPrefix + nameof(WriteConnectionCloseAfter);
		private const string SqlErrorCloseConnection = SqlClientPrefix + nameof(WriteConnectionCloseError);

		private const string SqlBeforeCommitTransaction = SqlClientPrefix + nameof(WriteTransactionCommitBefore);
		private const string SqlAfterCommitTransaction = SqlClientPrefix + nameof(WriteTransactionCommitAfter);
		private const string SqlErrorCommitTransaction = SqlClientPrefix + nameof(WriteTransactionCommitError);

		private const string SqlBeforeRollbackTransaction = SqlClientPrefix + nameof(WriteTransactionRollbackBefore);
		private const string SqlAfterRollbackTransaction = SqlClientPrefix + nameof(WriteTransactionRollbackAfter);
		private const string SqlErrorRollbackTransaction = SqlClientPrefix + nameof(WriteTransactionRollbackError);

		private static readonly object? EmptyStatistics = null;

		public static async Task<T> WithDiagnosticForCommand<T>(this DiagnosticListener @this, IEnumerable<IMySqlCommand> commands, Func<Task<T>> func)
		{
			var operationId = @this.WriteCommandBefore(commands);
			Exception? e = null;
			try
			{
				return await func().ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				e = ex;
				throw;
			}
			finally
			{
				if (e != null)
				{
					@this.WriteCommandError(operationId, commands, e);
				}
				else
				{
					@this.WriteCommandAfter(operationId, commands);
				}
			}
		}

		private static Guid WriteCommandBefore(this DiagnosticSource @this,
			IEnumerable<IMySqlCommand> sqlCommands,
			string operation = SqlClientExecuteScalarName)
		{
			if (!@this.IsEnabled(SqlBeforeExecuteCommand)) return Guid.Empty;
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

		private static void WriteCommandAfter(this DiagnosticSource @this,
			Guid operationId,
			IEnumerable<IMySqlCommand> sqlCommands,
			string operation = SqlClientExecuteScalarName)
		{
			if (!@this.IsEnabled(SqlAfterExecuteCommand)) return;
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
						Statistics = EmptyStatistics,
						Timestamp = Stopwatch.GetTimestamp()
					});
			}
		}

		private static void WriteCommandError(this DiagnosticSource @this,
			Guid operationId,
			IEnumerable<IMySqlCommand> sqlCommands,
			Exception ex, string operation = SqlClientExecuteScalarName)
		{
			if (!@this.IsEnabled(SqlErrorExecuteCommand)) return;
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

        public static async Task WithDiagnosticForConnectionOpen(this DiagnosticListener @this, MySqlConnection connection, Func<Task> func)
        {
	        var operationId = @this.WriteConnectionOpenBefore(connection);
	        Exception? e = null;
	        try
	        {
		        await func().ConfigureAwait(false);
	        }
	        catch (Exception ex)
	        {
		        e = ex;
		        throw;
	        }
	        finally
	        {
		        if (e != null)
		        {
			        @this.WriteConnectionOpenError(operationId, connection, e);
		        }
		        else
		        {
			        @this.WriteConnectionOpenAfter(operationId, connection);
		        }
	        }
        }

        private static Guid WriteConnectionOpenBefore(this DiagnosticSource @this,
	        MySqlConnection sqlConnection,
	        string operation = SqlClientOpenConnectionName)
        {
	        if (!@this.IsEnabled(SqlBeforeOpenConnection)) return Guid.Empty;
	        var operationId = Guid.NewGuid();

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

        private static void WriteConnectionOpenAfter(this DiagnosticSource @this,
	        Guid operationId,
	        MySqlConnection sqlConnection,
	        string operation = SqlClientOpenConnectionName)
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
                        Statistics = EmptyStatistics,
                        Timestamp = Stopwatch.GetTimestamp()
                    });
            }
        }

        private static void WriteConnectionOpenError(this DiagnosticSource @this,
	        Guid operationId,
	        MySqlConnection sqlConnection,
	        Exception ex, string operation = SqlClientOpenConnectionName)
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
        public static async Task WithDiagnosticForConnectionClose(this DiagnosticListener @this, MySqlConnection connection, Func<Task> func)
        {
	        var serverThread = GetConnectionId(connection);
	        var operationId = @this.WriteConnectionCloseBefore(connection);
	        Exception? e = null;
	        try
	        {
		        await func().ConfigureAwait(false);
	        }
	        catch (Exception ex)
	        {
		        e = ex;
		        throw;
	        }
	        finally
	        {
		        if (e != null)
		        {
			        @this.WriteConnectionCloseError(operationId, serverThread, connection, e);
		        }
		        else
		        {
			        @this.WriteConnectionCloseAfter(operationId, serverThread, connection);
		        }
	        }
        }

        private static Guid WriteConnectionCloseBefore(this DiagnosticSource @this,
	        MySqlConnection sqlConnection,
	        string operation = SqlClientCloseConnectionName)
        {
	        if (!@this.IsEnabled(SqlBeforeCloseConnection)) return Guid.Empty;
	        var operationId = Guid.NewGuid();

	        @this.Write(
		        SqlBeforeCloseConnection,
		        new
		        {
			        OperationId = operationId,
			        Operation = operation,
			        ConnectionId = sqlConnection.GetConnectionId(),
			        Connection = sqlConnection,
			        Statistics = EmptyStatistics,
			        Timestamp = Stopwatch.GetTimestamp()
		        });

	        return operationId;

        }

		private static void WriteConnectionCloseAfter(this DiagnosticSource @this,
			Guid operationId,
			int? clientConnectionId,
			MySqlConnection sqlConnection,
			string operation = SqlClientCloseConnectionName)
        {
            if (@this.IsEnabled(SqlAfterCloseConnection))
            {
                @this.Write(
                    SqlAfterCloseConnection,
                    new
                    {
                        OperationId = operationId,
                        Operation = operation,
                        ConnectionId = clientConnectionId,
                        Connection = sqlConnection,
                        Statistics = EmptyStatistics,
                        Timestamp = Stopwatch.GetTimestamp()
                    });
            }
        }

        private static void WriteConnectionCloseError(this DiagnosticSource @this,
	        Guid operationId,
	        int? clientConnectionId,
	        MySqlConnection sqlConnection,
	        Exception ex, string operation = SqlClientCloseConnectionName)
        {
            if (@this.IsEnabled(SqlErrorCloseConnection))
            {
                @this.Write(
                    SqlErrorCloseConnection,
                    new
                    {
                        OperationId = operationId,
                        Operation = operation,
                        ConnectionId = clientConnectionId,
                        Connection = sqlConnection,
                        Statistics = EmptyStatistics,
                        Exception = ex,
                        Timestamp = Stopwatch.GetTimestamp()
                    });
            }
        }

        public static async Task WithDiagnosticForCommit(this DiagnosticListener @this, IsolationLevel isolationLevel, MySqlConnection? connection, Func<Task> func)
        {
	        var operationId = @this.WriteTransactionCommitBefore(isolationLevel, connection);
	        Exception? e = null;
	        try
	        {
		        await func().ConfigureAwait(false);
	        }
	        catch (Exception ex)
	        {
		        e = ex;
		        throw;
	        }
	        finally
	        {
		        if (e != null)
		        {
			        @this.WriteTransactionCommitError(operationId, isolationLevel, connection, e);
		        }
		        else
		        {
			        @this.WriteTransactionCommitAfter(operationId, isolationLevel, connection);
		        }
	        }
        }

        private static Guid WriteTransactionCommitBefore(this DiagnosticListener @this, IsolationLevel isolationLevel, MySqlConnection? connection, string operation = SqlClientCommitName)
        {
	        if (!@this.IsEnabled(SqlBeforeCommitTransaction))
		        return Guid.Empty;
	        var operationId = Guid.NewGuid();

	        @this.Write(
		        SqlBeforeCommitTransaction,
		        new
		        {
			        OperationId = operationId,
			        Operation = operation,
			        IsolationLevel = isolationLevel,
			        Connection = connection,
			        Timestamp = Stopwatch.GetTimestamp()
		        });

	        return operationId;
        }

        private static void WriteTransactionCommitAfter(this DiagnosticSource @this,
	        Guid operationId,
	        IsolationLevel isolationLevel,
	        MySqlConnection? connection,
	        string operation = SqlClientCommitName)
        {
            if (@this.IsEnabled(SqlAfterCommitTransaction))
            {
                @this.Write(
                    SqlAfterCommitTransaction,
                    new
                    {
                        OperationId = operationId,
                        Operation = operation,
                        IsolationLevel = isolationLevel,
                        Connection = connection,
                        Timestamp = Stopwatch.GetTimestamp()
                    });
            }
        }

        private static void WriteTransactionCommitError(this DiagnosticSource @this,
	        Guid operationId,
	        IsolationLevel isolationLevel,
	        MySqlConnection? connection,
	        Exception ex, string operation = SqlClientCommitName)
        {
            if (@this.IsEnabled(SqlErrorCommitTransaction))
            {
                @this.Write(
                    SqlErrorCommitTransaction,
                    new
                    {
                        OperationId = operationId,
                        Operation = operation,
                        IsolationLevel = isolationLevel,
                        Connection = connection,
                        Exception = ex,
                        Timestamp = Stopwatch.GetTimestamp()
                    });
            }
        }

        public static async Task WithDiagnosticForRollback(this DiagnosticListener @this,
	        IsolationLevel isolationLevel, MySqlConnection? connection, Func<Task> func)
        {
	        var operationId = @this.WriteTransactionRollbackBefore(isolationLevel, connection);
	        Exception? e = null;
	        try
	        {
		        await func().ConfigureAwait(false);
	        }
	        catch (Exception ex)
	        {
		        e = ex;
		        throw;
	        }
	        finally
	        {
		        if (e != null)
		        {
			        @this.WriteTransactionRollbackError(operationId,isolationLevel, connection, e);
		        }
		        else
		        {
			        @this.WriteTransactionRollbackAfter(operationId, isolationLevel, connection);
		        }
	        }
        }

        private static Guid WriteTransactionRollbackBefore(this DiagnosticSource @this,
	        IsolationLevel isolationLevel,
	        MySqlConnection? connection,
	        string? transactionName = null, string operation = SqlClientRollbackName)
        {
	        if (!@this.IsEnabled(SqlBeforeRollbackTransaction)) return Guid.Empty;
	        var operationId = Guid.NewGuid();

	        @this.Write(
		        SqlBeforeRollbackTransaction,
		        new
		        {
			        OperationId = operationId,
			        Operation = operation,
			        IsolationLevel = isolationLevel,
			        Connection = connection,
			        TransactionName = transactionName,
			        Timestamp = Stopwatch.GetTimestamp()
		        });

	        return operationId;

        }

        private static void WriteTransactionRollbackAfter(this DiagnosticSource @this,
	        Guid operationId,
	        IsolationLevel isolationLevel,
	        MySqlConnection? connection,
	        string? transactionName = null, string operation = SqlClientRollbackName)
        {
            if (@this.IsEnabled(SqlAfterRollbackTransaction))
            {
                @this.Write(
                    SqlAfterRollbackTransaction,
                    new
                    {
                        OperationId = operationId,
                        Operation = operation,
                        IsolationLevel = isolationLevel,
                        Connection = connection,
                        TransactionName = transactionName,
                        Timestamp = Stopwatch.GetTimestamp()
                    });
            }
        }

        private static void WriteTransactionRollbackError(this DiagnosticSource @this,
	        Guid operationId,
	        IsolationLevel isolationLevel,
	        MySqlConnection? connection,
	        Exception ex, string? transactionName = null, string operation = SqlClientRollbackName)
        {
            if (@this.IsEnabled(SqlErrorRollbackTransaction))
            {
                @this.Write(
                    SqlErrorRollbackTransaction,
                    new
                    {
                        OperationId = operationId,
                        Operation = operation,
                        IsolationLevel = isolationLevel,
                        Connection = connection,
                        TransactionName = transactionName,
                        Exception = ex,
                        Timestamp = Stopwatch.GetTimestamp()
                    });
            }
        }
        private static int? GetConnectionId(this MySqlConnection @this) => @this.State == ConnectionState.Open ? (int?) @this.ServerThread : null;

	}
}
