using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace MySqlConnector.Tests
{
	public partial class DiagnosticTests
	{
		private class ListenerObservers : IObserver<DiagnosticListener>, IDisposable
		{
			public List<(string name, KeyValuePair<string, object>)> Events { get; } = new List<(string name, KeyValuePair<string, object>)>();
			private readonly List<IDisposable> m_subscriptions = new List<IDisposable>();
			public void OnCompleted()
			{
			}

			class EventObserver: IObserver<KeyValuePair<string, object>>
			{
				private readonly ListenerObservers m_parent;
				private readonly string m_streamName;

				public EventObserver(ListenerObservers parent, string streamName)
				{
					m_parent = parent;
					m_streamName = streamName;
				}


				public void OnCompleted()
				{
				}

				public void OnError(Exception error)
				{
				}

				public void OnNext(KeyValuePair<string, object> value)
				{
					m_parent.Events.Add((m_streamName, value));
				}
			}

			public void OnError(Exception error)
			{
			}

			public void OnNext(DiagnosticListener value)
			{
				m_subscriptions.Add(value.Subscribe(new EventObserver(this, value.Name)));
			}

			public void Dispose()
			{
				foreach (var subscription in m_subscriptions)
				{
					subscription.Dispose();
				}
			}
		}
	}
}
