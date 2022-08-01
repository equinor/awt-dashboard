import logo from './logo.svg';
import './App.css';
import TableSimple from './components/TableSimple';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <h1>
          AWT Dashboard
        </h1>
        <TableSimple />
      </header>
    </div>
  );
}

export default App;
