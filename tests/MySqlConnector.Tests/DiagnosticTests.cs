using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
		public async Task SendSqlAfterExecuteCommand()
		{
			await m_connection.OpenAsync();
			var command = m_connection.CreateCommand();
			command.CommandText = "SELECT 1";

			_ = await command.ExecuteReaderAsync();

			var operationId = GetOperationId("MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteCommandBefore");
			Assert.True(operationId.HasValue);

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteCommandAfter",
				(data) => data.Operation == "ExecuteScalar",
				(data) => data.ConnectionId == command.Connection?.ServerThread,
				(data) => data.OperationId == operationId,
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
				var operationId = GetOperationId("MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteCommandBefore");
				Assert.True(operationId.HasValue);

				Assert.Contains(m_observers.Events, Event(
					"MySqlClientDiagnosticListener",
					"MySql.Data.MySqlClient.WriteCommandError",
					(data) => data.Operation == "ExecuteScalar",
					(data) => data.ConnectionId == command.Connection?.ServerThread,
					(data) => AlmostNow(data.Timestamp),
					(data) => data.Exception == exception,
					(data) => data.OperationId == operationId,
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

			var operationId = GetOperationId("MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteConnectionOpenBefore");
			Assert.True(operationId.HasValue);

			Assert.Contains(m_observers.Events, Event(
				"MySqlClientDiagnosticListener",
				"MySql.Data.MySqlClient.WriteConnectionOpenAfter",
				(data) => data.Operation == "Open",
				(data) => data.Connection == m_connection,
				(data) => data.ConnectionId == m_connection?.ServerThread,
				(data) => data.OperationId == operationId,
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
				var operationId = GetOperationId("MySqlClientDiagnosticListener", "MySql.Data.MySqlClient.WriteConnectionOpenBefore");
				Assert.True(operationId.HasValue);

				Assert.Contains(m_observers.Events, Event(
					"MySqlClientDiagnosticListener",
					"MySql.Data.MySqlClient.WriteConnectionOpenError",
					(data) => data.Operation == "Open",
					(data) => data.Connection == connection,
					(data) => data.ConnectionId == (int?)null,
					(data) => data.OperationId == operationId,
					(data) => data.Exception == exception,
					(data) => AlmostNow(data.Timestamp)));
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

		private Guid? GetOperationId(string source, string eventName)
		{
			dynamic data =  m_observers.Events.Where(e => Event(source, eventName)(e)).Select(tuple=>tuple.Item2.Value).FirstOrDefault();
			return data != null ? (Guid?) data.OperationId : null;
		}

		private bool AlmostNow(long timestamp)
		{
			var now = Stopwatch.GetTimestamp();
			return ((now - timestamp) / Stopwatch.Frequency) < 1;
		}

		readonly FakeMySqlServer m_server;
		readonly MySqlConnectionStringBuilder m_csb;
		readonly ListenerObservers m_observers;
		readonly MySqlConnection m_connection;
		readonly IDisposable m_listenerSubscription;
	}
}
