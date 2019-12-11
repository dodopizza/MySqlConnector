using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Xunit;

namespace MySqlConnector.Tests
{
	public partial class DiagnosticTests : IDisposable
	{
		public void Dispose() {
			m_server.Stop();
			m_connection.Dispose();
			m_listenerSubscription.Dispose();
			m_observers.Dispose();
		}

		public DiagnosticTests()
		{
			m_server = new FakeMySqlServer();
			m_server.Start();

			m_csb = new MySqlConnectionStringBuilder
			{
				Server = "localhost",
				Port = (uint) m_server.Port,
			};
			m_observers = new ListenerObservers();
			m_listenerSubscription = DiagnosticListener.AllListeners.Subscribe(m_observers);
			m_connection = new MySqlConnection(m_csb.ConnectionString);

		}

		[Fact]
		public async Task SendSqlBeforeExecuteCommand()
		{
			await m_connection.OpenAsync();
			var command = m_connection.CreateCommand();
			command.CommandText = "SELECT 1";

			_ = await command.ExecuteReaderAsync();

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteCommandBefore",
				(data) => data.Operation == "ExecuteScalar",
				(data) => data.ConnectionId == command.Connection?.ServerThread,
				(data) => data.GetType().GetProperty("OperationId") != null,
				(data) => data.Command == command));
		}


		[Fact]
		public void SendSqlAfterExecuteCommand()
		{
			m_connection.OpenAsync().Wait();
			var command = m_connection.CreateCommand();
			command.CommandText = "SELECT 1";

			var _ = command.ExecuteReaderAsync().Result;

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteCommandAfter",
				(data) => data.Operation == "ExecuteScalar",
				(data) => data.ConnectionId == command.Connection?.ServerThread,
				(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteCommandBefore"),
				(data) => AlmostNow(data.Timestamp),
				(data) => data.Statistics == null, //TODO: implement stats same as https://github.com/dotnet/runtime/blob/master/src/libraries/System.Data.SqlClient/src/System/Data/SqlClient/SqlStatistics.cs
				(data) => data.Command == command));
		}

		[Fact]
		public async Task SendSqlErrorExecuteCommand()
		{
			await m_connection.OpenAsync();
			var command = m_connection.CreateCommand();
			command.CommandText = "some invalid sql";

			try
			{
				_ = await command.ExecuteReaderAsync();
			}
			catch (Exception exception)
			{

				Assert.Contains(m_observers.Events, Event(
					"MySqlClientDiagnosticListener",
					"MySql.Data.MySqlClient.WriteCommandError",
					(data) => data.Operation == "ExecuteScalar",
					(data) => data.ConnectionId == command.Connection?.ServerThread,
					(data) => AlmostNow(data.Timestamp),
					(data) => data.Exception == exception,
					(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteCommandBefore"),
					(data) => data.Command == command));
			}
		}

		[Fact]
		public async Task SendSqlBeforeOpenConnection()
		{
			await m_connection.OpenAsync();

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteConnectionOpenBefore",
				(data) => data.Operation == "Open",
				(data) => data.Connection == m_connection,
				(data) => data.GetType().GetProperty("OperationId") != null,
				(data) => AlmostNow(data.Timestamp)));
		}


		[Fact]
		public async Task SendSqlAfterOpenConnection()
		{
			await m_connection.OpenAsync();

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteConnectionOpenAfter",
				(data) => data.Operation == "Open",
				(data) => data.Connection == m_connection,
				(data) => data.ConnectionId == m_connection?.ServerThread,
				(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteConnectionOpenBefore"),
				(data) => data.Statistics == null, //TODO: implement stats same as https://github.com/dotnet/runtime/blob/master/src/libraries/System.Data.SqlClient/src/System/Data/SqlClient/SqlStatistics.cs
				(data) => AlmostNow(data.Timestamp)));
		}

		[Fact]
		public async Task SendSqlErrorOpenConnection()
		{
			var connection = new MySqlConnection("some error connection string");
			try
			{
				await connection.OpenAsync();
			}
			catch (Exception exception)
			{

				Assert.Contains(m_observers.Events, Event(
					"MySqlClientDiagnosticListener",
					"MySql.Data.MySqlClient.WriteConnectionOpenError",
					(data) => data.Operation == "Open",
					(data) => data.Connection == connection,
					(data) => data.ConnectionId == (int?)null,
					(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteConnectionOpenBefore"),
					(data) => data.Exception == exception,
					(data) => AlmostNow(data.Timestamp)));
			}
		}

		[Fact]
		public async Task SendSqlBeforeCloseConnection()
		{
			await m_connection.OpenAsync();
			var connectionId = m_connection.ServerThread;
			await m_connection.CloseAsync();

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteConnectionCloseBefore",
				(data) => data.Operation == "Close",
				(data) => data.Connection == m_connection,
				(data) => data.ConnectionId  == connectionId,
				(data) => data.GetType().GetProperty("OperationId") != null,
				(data) => data.Statistics == null, //TODO: implement stats same as https://github.com/dotnet/runtime/blob/master/src/libraries/System.Data.SqlClient/src/System/Data/SqlClient/SqlStatistics.cs
				(data) => AlmostNow(data.Timestamp)));
		}

		[Fact]
		public async Task SendSqlAfterCloseConnection()
		{
			await m_connection.OpenAsync();
			var connectionId = m_connection.ServerThread;
			await m_connection.CloseAsync();
			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteConnectionCloseAfter",
				(data) => data.Operation == "Close",
				(data) => data.Connection == m_connection,
				(data) => data.ConnectionId  == connectionId,
				(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteConnectionCloseBefore"),
				(data) => data.Statistics == null, //TODO: implement stats same as https://github.com/dotnet/runtime/blob/master/src/libraries/System.Data.SqlClient/src/System/Data/SqlClient/SqlStatistics.cs
				(data) => AlmostNow(data.Timestamp)));
		}

		[Fact]
		public async Task SendSqlErrorCloseConnection()
		{
			await m_connection.OpenAsync();
			var connectionId = m_connection.ServerThread;
			await m_connection.BeginTransactionAsync();
			try
			{
				m_server.Stop();
				await m_connection.CloseAsync();
			}
			catch (Exception exception)
			{
				Assert.Contains(m_observers.Events, Event(
					"MySqlClientDiagnosticListener",
					"MySql.Data.MySqlClient.WriteConnectionCloseError",
					(data) => data.Operation == "Close",
					(data) => data.Connection == m_connection,
					(data) => data.ConnectionId == connectionId,
					(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteConnectionCloseBefore"),
					(data) => data.Statistics == null, //TODO: implement stats same as https://github.com/dotnet/runtime/blob/master/src/libraries/System.Data.SqlClient/src/System/Data/SqlClient/SqlStatistics.cs
					(data) => data.Exception == exception,
					(data) => AlmostNow(data.Timestamp)));
			}
		}

		[Fact]
		public async Task SendSqlBeforeCommitTransaction()
		{
			await m_connection.OpenAsync();
			var transaction = await m_connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);

			await transaction.CommitAsync();

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteTransactionCommitBefore",
				(data) => data.Operation == "Commit",
				(data) => data.GetType().GetProperty("OperationId") != null,
				(data) => data.Connection == m_connection,
				(data) => AlmostNow(data.Timestamp),
				(data) => data.IsolationLevel == IsolationLevel.ReadCommitted));
		}


		[Fact]
		public async void SendSqlAfterCommitTransaction()
		{
			await m_connection.OpenAsync();
			var transaction = await m_connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);

			await transaction.CommitAsync();

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteTransactionCommitAfter",
				(data) => data.Operation == "Commit",
				(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteTransactionCommitBefore"),
				(data) => data.Connection == m_connection,
				(data) => AlmostNow(data.Timestamp),
				(data) => data.IsolationLevel == IsolationLevel.ReadCommitted));
		}

		[Fact]
		public async Task SendSqlErrorCommitTransaction()
		{
			await m_connection.OpenAsync();
			var transaction = await m_connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);

			try
			{
				m_server.Stop();
				await transaction.CommitAsync();
			}
			catch (Exception exception)
			{

				Assert.Contains(m_observers.Events, Event(
					"MySqlClientDiagnosticListener",
					"MySql.Data.MySqlClient.WriteTransactionCommitError",
					(data) => data.Operation == "Commit",
					(data) => data.Exception == exception,
					(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteTransactionCommitBefore"),
					(data) => data.Connection == m_connection,
					(data) => AlmostNow(data.Timestamp),
					(data) => data.IsolationLevel == IsolationLevel.ReadCommitted));
			}
		}

		[Fact]
		public async Task SendSqlBeforeRollbackTransaction()
		{
			await m_connection.OpenAsync();
			var transaction = await m_connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);

			await transaction.RollbackAsync();

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteTransactionRollbackBefore",
				(data) => data.Operation == "Rollback",
				(data) => data.GetType().GetProperty("OperationId") != null,
				(data) => data.Connection == m_connection,
				(data) => AlmostNow(data.Timestamp),
				(data) => data.TransactionName == (string)null, //SqlClient supports named transaction rollback. We dont support it. Added Just for compatibility.
				(data) => data.IsolationLevel == IsolationLevel.ReadCommitted));
		}

		[Fact]
		public async void SendSqlAfterRollbackTransaction()
		{
			await m_connection.OpenAsync();
			var transaction = await m_connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);

			await transaction.RollbackAsync();

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteTransactionRollbackAfter",
				(data) => data.Operation == "Rollback",
				(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteTransactionRollbackBefore"),
				(data) => data.Connection == m_connection,
				(data) => data.TransactionName == (string)null, //SqlClient supports named transaction rollback. We dont support it. Added Just for compatibility.
				(data) => AlmostNow(data.Timestamp),
				(data) => data.IsolationLevel == IsolationLevel.ReadCommitted));
		}

		[Fact]
		public async Task SendSqlErrorRollbackTransaction()
		{
			await m_connection.OpenAsync();
			var transaction = await m_connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);

			try
			{
				m_server.Stop();
				await transaction.RollbackAsync();
			}
			catch (Exception exception)
			{

				Assert.Contains(m_observers.Events, Event(
					"MySqlClientDiagnosticListener",
					"MySql.Data.MySqlClient.WriteTransactionRollbackError",
					(data) => data.Operation == "Rollback",
					(data) => data.Exception == exception,
					(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteTransactionRollbackBefore"),
					(data) => data.TransactionName == (string)null, //SqlClient supports named transaction rollback. We dont support it. Added Just for compatibility.
					(data) => data.Connection == m_connection,
					(data) => AlmostNow(data.Timestamp),
					(data) => data.IsolationLevel == IsolationLevel.ReadCommitted));
			}
		}

		[Fact]
		public async void SendSqlAfterRollbackTransactionOnDispose()
		{
			await m_connection.OpenAsync();
			using (await m_connection.BeginTransactionAsync(IsolationLevel.ReadCommitted)){}

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteTransactionRollbackAfter",
				(data) => data.Operation == "Rollback",
				(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteTransactionRollbackBefore"),
				(data) => data.Connection == m_connection,
				(data) => data.TransactionName == (string)null, //SqlClient supports named transaction rollback. We dont support it. Added Just for compatibility.
				(data) => AlmostNow(data.Timestamp),
				(data) => data.IsolationLevel == IsolationLevel.ReadCommitted));
		}

		[Fact]
		public async Task SendSqlErrorRollbackTransactionOnDispose()
		{
			await m_connection.OpenAsync();
			var transaction = await m_connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);

			try
			{
				m_server.Stop();
				using (transaction){}
			}
			catch (Exception exception)
			{

				Assert.Contains(m_observers.Events, Event(
					"MySqlClientDiagnosticListener",
					"MySql.Data.MySqlClient.WriteTransactionRollbackError",
					(data) => data.Operation == "Rollback",
					(data) => data.Exception == exception,
					(data) => CorrectOperationId(data.OperationId, "MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteTransactionRollbackBefore"),
					(data) => data.TransactionName == (string)null, //SqlClient supports named transaction rollback. We dont support it. Added Just for compatibility.
					(data) => data.Connection == m_connection,
					(data) => AlmostNow(data.Timestamp),
					(data) => data.IsolationLevel == IsolationLevel.ReadCommitted));
			}
		}

		private Predicate<(string name, KeyValuePair<string, object>)> Event(string source, string eventName, params Predicate<dynamic>[] matchers)
		{
			return tuple =>
			{
				var (actualSource, (actualEventName, data)) = tuple;
				if (source != actualSource) return false;
				if (eventName != actualEventName) return false;
				return matchers.All(matcher => matcher((dynamic) data));
			};
		}

		private bool CorrectOperationId(Guid operationId, string source, string parentEventName) =>
			m_observers.Events.Any(e =>
				Event(source, parentEventName, data => data.OperationId == operationId)(e));

		private bool AlmostNow(long timestamp)
		{
			var now = Stopwatch.GetTimestamp();
			var rawElapsedTicks = now - timestamp;
			if (Stopwatch.IsHighResolution)
				rawElapsedTicks =  (long) (rawElapsedTicks * (10000000.0/ Stopwatch.Frequency));
			var elapsed = new TimeSpan(rawElapsedTicks);
			return elapsed.Minutes < 1;
		}

		readonly FakeMySqlServer m_server;
		readonly MySqlConnectionStringBuilder m_csb;
		readonly ListenerObservers m_observers;
		readonly MySqlConnection m_connection;
		readonly IDisposable m_listenerSubscription;
	}
}
