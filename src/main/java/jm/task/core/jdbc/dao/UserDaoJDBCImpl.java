package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl extends Util implements UserDao {
    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        Connection connection = getConnection();
        Statement statement = null;
        try {
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            statement.execute("CREATE TABLE `users`.`users` (\n" +
                    "  `id` INT NOT NULL AUTO_INCREMENT,\n" +
                    "  `name` VARCHAR(45) NOT NULL,\n" +
                    "  `lastName` VARCHAR(45) NOT NULL,\n" +
                    "  `age` INT NOT NULL,\n" +
                    "  PRIMARY KEY (`id`))");
            connection.commit();
            System.out.println("Таблица users добавлена");
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
        }
    }

    public void dropUsersTable() {
        Connection connection = getConnection();
        Statement statement = null;
        try {
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            statement.execute("DROP TABLE IF EXISTS users");
            connection.commit();
            System.out.println("Таблица users удалена, либо ее не было");
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        Connection connection = getConnection();
        PreparedStatement statement = null;
        try {
            connection.setAutoCommit(false);
            statement = connection.prepareStatement("INSERT INTO users (name, lastName, age) VALUES (?,?,?)");
            statement.setString(1, name);
            statement.setString(2, lastName);
            statement.setByte(3, age);
            statement.execute();
            connection.commit();
            System.out.println("User с именем- " + name + " добавлен в базу данных");
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
        }
    }

    public void removeUserById(long id) {
        Connection connection = getConnection();
        PreparedStatement statement = null;
        try {
            connection.setAutoCommit(false);
            statement = connection.prepareStatement("DELETE FROM users WHERE id=?");
            statement.setLong(1, id);
            statement.execute();
            connection.commit();
            System.out.println("Пользователь с ID = " + id + " удален");
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
        }

    }

    public List<User> getAllUsers() {
        Connection connection = getConnection();
        List<User> userList = new ArrayList<>();
        Statement statement = null;
        try {
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            ResultSet result = statement.executeQuery("SELECT id, name, lastName, age FROM users");
            connection.commit();
            while (result.next()) {
                User user = new User();
                user.setId(result.getLong("id"));
                user.setName(result.getString("name"));
                user.setLastName(result.getString("lastName"));
                user.setAge(result.getByte("age"));
                userList.add(user);
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
        }
        System.out.println("Пользователи добавлены в коллекцию");
        return userList;

    }

    public void cleanUsersTable() {
        Connection connection = getConnection();
        Statement statement = null;
        String sql = "TRUNCATE TABLE users;";
        try {
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            statement.execute("TRUNCATE TABLE users");
            connection.commit();
            System.out.println("Таблица users очищена");
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
        }

    }
}
