using System;
using System.Collections.Generic;
using System.Text;

namespace RpcClientApp
{
    public class ScopedConsoleColour : IDisposable
    {
        private ConsoleColor oldColour;

        public ScopedConsoleColour(ConsoleColor newColour)
        {
            this.oldColour = Console.ForegroundColor;

            Console.ForegroundColor = newColour;
        }

        public void Dispose()
        {
            Console.ForegroundColor = this.oldColour;
        }
    }
}
